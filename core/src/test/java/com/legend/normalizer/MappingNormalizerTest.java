package com.legend.normalizer;

import com.legend.compiler.RelationalKinds;

import com.legend.model.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.protocol.Multiplicity;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import com.legend.model.FunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.protocol.Realization;
import com.legend.model.PackageableElement;
import com.legend.model.SynthHat;
import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
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

    /** A built call's simple name: since L7 "resolve once" step 3 (2026-10-10) a call the normalizer builds is born
     *  resolved, so its spelling may be the full name the resolver gives it; these tests assert WHICH function is
     *  called, not how the resolver spells it. */
    private static String fn(com.legend.protocol.spec.AppliedFunction af) {
        String f = af.function();
        int cut = f.lastIndexOf("::");
        return cut < 0 ? f : f.substring(cut + 2);
    }

    // ====================================================================
    // 1 & 2 — Foundation
    // ====================================================================

    @Test
    void passthroughForNonMappingElements() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firstName: String[1]; }");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        assertEquals(1, normalized.elements().size());
        assertSame(parsed.elements().get(0), normalized.elements().get(0),
                "Non-Mapping elements should be referentially identical (not rebuilt)");
    }

    @Test
    void preNormalize_noLiftedFunctions_postNormalize_oneFnPerClassMapping() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Class model::RawFirm   { legalName: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
            assertEquals("map", fn(map),
                    "every synth body terminates in map(...)");
            LambdaFunction lambda = (LambdaFunction) map.parameters().get(1);
            AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
            assertEquals("new", fn(newCall),
                    "every map lambda body is ^Target(...) ie new()");
        }
    }

    // ====================================================================
    // M3 — canonical binding table (surface -> canonical rewrite)
    // ====================================================================

    @Test
    @DisplayName("M3: legacy mapping rewrites to a canonical binding table; bindings point at the lifted FQNs")
    void legacyRewritesToCanonicalBindingTable() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Class model::RawFirm   { legalName: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person:    Pure { ~src model::RawPerson name: $src.name } "
                        + "   model::Firm:      Pure { ~src model::RawFirm   legalName: $src.legalName } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        var md = canonicalMapping(normalized, "my::M");

        assertEquals(2, md.classBindings().size(), "one binding per class mapping, in order");
        var personBinding = md.classBindings().get(0);
        assertEquals("model::Person", personBinding.classFqn());
        assertInstanceOf(com.legend.model.MappingDefinition.ClassBinding.Pure.class, personBinding);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { id: Integer[1]; } "
                        + "Class model::Person { firmId: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_FIRM   (ID INTEGER) "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "function acme::funcs::personMapping(): model::Person[*] "
                        + "  { model::Person.all() } "
                        + "\n###Mapping\nMapping acme::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase acme::DB "
                        + "( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping acme::M ( "
                        + "  *model::Person: Relational { #>{acme::DB.T_PERSON}#"
                        + " -> map(r | ^model::Person(name ="
                        + " $r.NAME->meta::legend::lite::trustOne())) } "
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
        assertEquals("acme::M$class$model::Person", b.functionFqn(),
                "no inline realization survives Phase E — the compiled"
                        + " binding carries the lifted function's FQN");
    }

    @Test
    @DisplayName("M5: an inline association lambda lifts to a 2-param Boolean predicate typed from the ends")
    void cleanSheetInlineAssociationIsLifted() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmId: Integer[1]; } "
                        + "Class model::Firm { id: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "\n###Mapping\nMapping acme::M ( "
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
        assertEquals("acme::M$assoc$model::Person_Firm",
                md.associationBindings().get(0).predicateFunctionFqn());
    }

    @Test
    @DisplayName("M5: inline works for Pure (M2M) bindings too — same lift, kind-agnostic")
    void cleanSheetInlinePureBindingIsLifted() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "\n###Mapping\nMapping acme::M ( "
                        + "  *model::Person: Pure { model::RawPerson.all() } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);
        assertEquals("acme::M$class$model::Person", fn.qualifiedName());
        assertEquals(Multiplicity.Concrete.ZERO_MANY, fn.returnMultiplicity());
        // The kind tag stays PURE on the binding; the lift didn't inspect it.
        var b = canonicalMapping(normalized, "acme::M").classBindings().get(0);
        assertInstanceOf(MappingDefinition.ClassBinding.Pure.class, b);
        assertEquals("acme::M$class$model::Person", b.functionFqn());
    }

    @Test
    @DisplayName("M5: §7.4 guard — no inline realization survives a NormalizedModel")
    void noInlineRealizationSurvivesPhaseE() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase acme::DB "
                        + "( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping acme::M ( "
                        + "  *model::Person: Relational { #>{acme::DB.T_PERSON}#"
                        + " -> map(r | ^model::Person(name ="
                        + " $r.NAME->meta::legend::lite::trustOne())) } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        // §7.4 is now a TYPE guarantee: compiled bindings carry function
        // FQNs only (no Realization union exists post-E). Assert the FQNs
        // resolve to lifted/user functions.
        for (PackageableElement el : normalized.elements()) {
            if (el instanceof MappingDefinition cmd) {
                for (var b : cmd.classBindings()) {
                    assertFalse(b.functionFqn().isEmpty(),
                            "class binding carries a function FQN post-E");
                }
                for (var b : cmd.associationBindings()) {
                    assertFalse(b.predicateFunctionFqn().isEmpty(),
                            "association binding carries a predicate FQN post-E");
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { fullName: String[1]; age: Integer[1]; } "
                        + "Class model::RawPerson { firstName: String[1]; lastName: String[1]; age: Integer[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("map", fn(mapCall));
        assertEquals(2, mapCall.parameters().size());

        AppliedFunction getAll = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("getAll", fn(getAll));
        assertEquals(new PackageableElementPtr("model::RawPerson"),
                getAll.parameters().get(0));

        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        assertEquals(List.of(new Variable("src")), lambda.parameters());

        // ^Person(fullName=..., age=...)
        AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
        assertEquals("new", fn(newCall));
        assertEquals(new PackageableElementPtr("model::Person"), newCall.parameters().get(0));
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        assertEquals("model::Person", ni.className());
        assertEquals(List.of("fullName", "age"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "Property keys preserve declaration order");

        // Each KeyExpression carries the verbatim parsed expression
        // (isAdd=false for '=' bindings).
        for (KeyExpression ke : ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList()) {
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
            assertSame(pb.expression(), ni.first(pb.propertyName()).value(),
                    "M2M PropertyBinding expression must be the same object "
                            + "(no rebuild) in the synth fn body; property="
                            + pb.propertyName());
        }

        // age's value is the bare $src.age property access -- spot check the
        // structural shape (not just identity) so a parser regression that
        // changes shape would also surface here.
        ValueSpecification ageExpr = ni.first("age").value();
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firstName: String[1]; lastName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (FIRST VARCHAR(50), LAST VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("map", fn(mapCall));

        AppliedFunction tableRef = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));
        assertEquals(2, tableRef.parameters().size());
        // Args are PackageableElementPtr(db) + CString(table) — query-parser
        // parity (H1: TableReferenceChecker serves both surfaces).
        assertEquals(new PackageableElementPtr("db::DB"),
                tableRef.parameters().get(0));
        assertEquals(new com.legend.protocol.spec.CString("T_PERSON"),
                tableRef.parameters().get(1));

        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        assertEquals(List.of(new Variable("row")), lambda.parameters());

        AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
        assertEquals("new", fn(newCall));
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        assertEquals("model::Person", ni.className());
        assertEquals(List.of("firstName", "lastName"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));

        // firstName value is $row.FIRST (AppliedProperty(receiver=row, property=FIRST))
        AppliedProperty firstName = (AppliedProperty) toOneInner(ni.first("firstName").value());
        assertEquals(new Variable("row"), firstName.receiver());
        assertEquals("FIRST", firstName.property());

        AppliedProperty lastName = (AppliedProperty) toOneInner(ni.first("lastName").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "\n###Mapping\nMapping pkg::M::PersonMap ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::ActivePerson { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; isActive: Boolean[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("map", fn(mapCall));

        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", fn(filterCall),
                "M2M filter wraps the getAll source, lives BEFORE the map terminus");

        AppliedFunction getAll = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals("getAll", fn(getAll));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), IS_ACTIVE BIT) "
                        + "  Filter ActiveOnly( T_PERSON.IS_ACTIVE = 1 ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] ActiveOnly "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // Body: map(filter(tableReference(...), row | $row.IS_ACTIVE == 1), row | ^Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", fn(mapCall));

        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", fn(filterCall),
                "Relational ~filter Direct wraps tableReference");

        AppliedFunction tableRef = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));

        // The filter lambda binds 'row' and the body is the translated
        // RelationalOperation: equal($row.IS_ACTIVE, 1)
        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        assertEquals(List.of(new Variable("row")), filterLambda.parameters());
        AppliedFunction condition = (AppliedFunction) sole(filterLambda.body());
        assertEquals("equal", fn(condition),
                "T_PERSON.IS_ACTIVE = 1 translates to equal(...)");
        assertEquals(2, condition.parameters().size());
        // left = $row.IS_ACTIVE->toOne() — a relational comparison over a
        // column conforms by EMISSION (SQL null semantics; no [0..1]
        // guard conjunct in the lowered filter)
        AppliedFunction leftToOne = (AppliedFunction) condition.parameters().get(0);
        assertEquals(Pure.Lite.TRUST_ONE, leftToOne.function(),
                "synth conformance spells trustOne (audit slice 3)");
        AppliedProperty left = (AppliedProperty) leftToOne.parameters().get(0);
        assertEquals("IS_ACTIVE", left.property());
        assertEquals(new Variable("row"), left.receiver());
        // right = 1 (CInteger)
        assertEquals(new com.legend.protocol.spec.CInteger(1L),
                condition.parameters().get(1));
    }

    @Test
    void relationalFilter_unknownDatabaseThrows() {
        // ~filter [other::DB] ActiveOnly  - the cross-db filter pointer
        // references a database that isn't in the model. Phase E should
        // throw clearly.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] NonexistentFilter "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::DeptInfo { name: String[1]; location: String[1]; } "
                        + "Class model::Department { name: String[1]; location: String[1]; } "
                        + "Class model::Employee { firstName: String[1]; department: model::Department[*]; } "
                        + "Class model::StaffWithDept { fullName: String[1]; department: model::DeptInfo[*]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
        var fullNameValue = ni.first("fullName").value();
        AppliedProperty fullNameProp = assertInstanceOf(AppliedProperty.class, fullNameValue);
        assertEquals("firstName", fullNameProp.property());
        assertEquals(new Variable("src"), fullNameProp.receiver());

        // department is class-typed: should be NewInstanceCast over $src.department.
        var deptValue = ni.first("department").value();
        com.legend.protocol.spec.NewInstanceCast deptCast =
                assertInstanceOf(com.legend.protocol.spec.NewInstanceCast.class, deptValue,
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::DeptInfo { name: String[1]; } "
                        + "Class model::Department { name: String[1]; } "
                        + "Class model::Employee { department: model::Department[*]; } "
                        + "Class model::StaffWithDept { department: model::DeptInfo[*]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { b: model::B[1]; } "
                        + "Class model::B { a: model::A[1]; } "
                        + "Class model::SrcA { b: model::SrcB[1]; } "
                        + "Class model::SrcB { a: model::SrcA[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
            com.legend.protocol.spec.NewInstanceCast cast =
                    assertInstanceOf(com.legend.protocol.spec.NewInstanceCast.class,
                            ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList().iterator().next().value(),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { fullName: String[1]; } "
                        + "Class model::RawPerson { firstName: String[1]; lastName: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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

        assertSame(pb.expression(), ni.first("fullName").value(),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // Body shape: map(join(tableRef, ColSpec, cond), row | ^Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", fn(mapCall));

        AppliedFunction join = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, join.function());
        assertEquals(3, join.parameters().size(),
                "join takes (rel, ColSpec, cond)");

        // Upstream is the bare tableReference (no filter, no prior join)
        AppliedFunction upstream = (AppliedFunction) join.parameters().get(0);
        assertEquals("tableReference", fn(upstream));

        // ColSpec(~Person_Firm: tableRef(db::DB, T_FIRM))
        com.legend.protocol.spec.ColSpec binding =
                (com.legend.protocol.spec.ColSpec) join.parameters().get(1);
        assertEquals("Person_Firm", binding.name(),
                "single-hop alias is the join name");
        LambdaFunction targetLambda = binding.function1();
        assertEquals(List.of(), targetLambda.parameters(),
                "ColSpec target lambda is 0-param (engine convention)");
        AppliedFunction targetTableRef = (AppliedFunction) sole(targetLambda.body());
        assertEquals("tableReference", fn(targetTableRef));
        assertEquals(new PackageableElementPtr("db::DB"),
                targetTableRef.parameters().get(0));
        assertEquals(new com.legend.protocol.spec.CString("T_FIRM"),
                targetTableRef.parameters().get(1));

        // Condition lambda: {s, t | $s.FIRM_ID == $t.ID}
        LambdaFunction condLambda = (LambdaFunction) join.parameters().get(2);
        assertEquals(List.of(new Variable("s"), new Variable("t")),
                condLambda.parameters());
        AppliedFunction cond = (AppliedFunction) sole(condLambda.body());
        assertEquals("equal", fn(cond));
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
        AppliedProperty firmName = (AppliedProperty) toOneInner(ni.first("firmName").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { orgName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID   = T_ORG.ID  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.JOIN_SLOT, outerJoin.function(),
                "outer join is hop 2");
        AppliedFunction innerJoin = (AppliedFunction) outerJoin.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, innerJoin.function(),
                "inner join is hop 1");
        AppliedFunction tableRef = (AppliedFunction) innerJoin.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));

        // Hop 1: alias Person_Firm, target T_FIRM, cond $s.FIRM_ID = $t.ID
        com.legend.protocol.spec.ColSpec hop1Spec =
                (com.legend.protocol.spec.ColSpec) innerJoin.parameters().get(1);
        assertEquals("Person_Firm", hop1Spec.name());
        AppliedFunction hop1Target = (AppliedFunction) sole(hop1Spec.function1().body());
        assertEquals(new com.legend.protocol.spec.CString("T_FIRM"),
                hop1Target.parameters().get(1));

        // Hop 2: alias Person_Firm__Firm_Org, target T_ORG
        com.legend.protocol.spec.ColSpec hop2Spec =
                (com.legend.protocol.spec.ColSpec) outerJoin.parameters().get(1);
        assertEquals("Person_Firm__Firm_Org", hop2Spec.name());
        AppliedFunction hop2Target = (AppliedFunction) sole(hop2Spec.function1().body());
        assertEquals(new com.legend.protocol.spec.CString("T_ORG"),
                hop2Target.parameters().get(1));

        // Hop 2 condition: $s.Person_Firm.ORG_ID == $t.ID
        // This is the key multi-hop invariant: the source at hop 2 sees
        // hop 1's target as a sub-row reached through the hop 1 alias.
        LambdaFunction hop2Cond = (LambdaFunction) outerJoin.parameters().get(2);
        AppliedFunction hop2Eq = (AppliedFunction) sole(hop2Cond.body());
        assertEquals("equal", fn(hop2Eq));
        // Left: $s.Person_Firm.ORG_ID
        AppliedProperty left = (AppliedProperty) hop2Eq.parameters().get(0);
        assertEquals("ORG_ID", left.property());
        AppliedProperty leftSub = (AppliedProperty) left.receiver();
        assertEquals("Person_Firm", leftSub.property(),
                "Hop 2 cond's left side reaches T_FIRM via $s.Person_Firm");
        assertEquals(new Variable("s"), leftSub.receiver());
        // Right: $t.ID
        AppliedProperty right = (AppliedProperty) hop2Eq.parameters().get(1);
        assertEquals("ID", right.property());
        assertEquals(new Variable("t"), right.receiver());

        // Project terminal: $row.orgName_h2.NAME
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        AppliedProperty orgName = (AppliedProperty) toOneInner(ni.first("orgName").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { managerId: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, MGR_ID INTEGER) "
                        + "  Join Person_Manager( T_PERSON.MGR_ID = {target}.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    managerId: [db::DB] @Person_Manager | T_PERSON.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function());

        // Target table of the self-join is T_PERSON (same as source).
        com.legend.protocol.spec.ColSpec spec =
                (com.legend.protocol.spec.ColSpec) joinCall.parameters().get(1);
        AppliedFunction targetTableRef = (AppliedFunction) sole(spec.function1().body());
        assertEquals(new com.legend.protocol.spec.CString("T_PERSON"),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { legalName: String[1]; revenue: Integer[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50), REVENUE INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("map", fn(mapCall));

        // map's source is the legacyNavigate pipeline step.
        AppliedFunction nav = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.LEGACY_NAVIGATE, nav.function(),
                "Class-typed Join PM with mapped target emits a legacyNavigate step");
        assertEquals(4, nav.parameters().size(),
                "legacyNavigate takes (rel, ColSpec, targetRows, cond)");

        // Upstream of legacyNavigate is the bare tableReference.
        AppliedFunction upstream = (AppliedFunction) nav.parameters().get(0);
        assertEquals("tableReference", fn(upstream));

        // ColSpec(~firm: {| getAll(model::Firm)}) — slot named after the property.
        com.legend.protocol.spec.ColSpec slot =
                (com.legend.protocol.spec.ColSpec) nav.parameters().get(1);
        assertEquals("firm", slot.name(),
                "legacyNavigate slot is the PM property name");
        LambdaFunction slotLambda = slot.function1();
        assertEquals(List.of(), slotLambda.parameters(),
                "slot body is a 0-param lambda");
        AppliedFunction getAll = (AppliedFunction) sole(slotLambda.body());
        assertEquals("getAll", fn(getAll),
                "slot materializes the target class via getAll(Firm)");
        PackageableElementPtr firmPtr = (PackageableElementPtr) sole(getAll.parameters());
        assertEquals("model::Firm", firmPtr.fullPath());

        // Condition lambda: {s, t | $s.FIRM_ID == $t.ID}.
        LambdaFunction condLambda = (LambdaFunction) nav.parameters().get(3);
        assertEquals(2, condLambda.parameters().size());
        AppliedFunction eq = (AppliedFunction) sole(condLambda.body());
        assertEquals("equal", fn(eq));
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
                List.copyOf(personNi.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "Ctor has primitive `name` and class-typed `firm` (read from the slot)");

        KeyExpression firmKe = personNi.first("firm");
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { legalName: String[1]; employeeCount: Integer[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_NAME VARCHAR(50), FIRM_COUNT INTEGER) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("tableReference", fn(underneath),
                "Embedded PMs do NOT emit associates; main table is the direct upstream");

        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance personNi = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);

        // firm value is ^Firm(...).
        AppliedFunction firmNew = (AppliedFunction) personNi.first("firm").value();
        assertEquals("new", fn(firmNew));
        assertEquals(new PackageableElementPtr("model::Firm"), firmNew.parameters().get(0));
        NewInstance firmNi = (NewInstance) firmNew.parameters().get(1);
        assertEquals("model::Firm", firmNi.className());
        assertEquals(List.of("legalName", "employeeCount"),
                List.copyOf(firmNi.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));

        // legalName reads $row.FIRM_NAME (outer table, NOT a join alias).
        AppliedProperty legalName = (AppliedProperty) toOneInner(firmNi.first("legalName").value());
        assertEquals("FIRM_NAME", legalName.property());
        assertEquals(new Variable("row"), legalName.receiver(),
                "Embedded sub-PM reads from outer $row, not a join alias");

        AppliedProperty empCount = (AppliedProperty) toOneInner(firmNi.first("employeeCount").value());
        assertEquals("FIRM_COUNT", empCount.property());
        assertEquals(new Variable("row"), empCount.receiver());
    }

    @Test
    void embeddedPM_innerJoinSubPM_throws() {
        // firm ( legalName: T_PERSON.FIRM_NAME, owner: @Person_Other )
        // The Embedded contains a Join PM sub-PM, which would need a
        // hoisted associate -- not yet supported in value-position embeds.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Other  { name: String[1]; } "
                        + "Class model::Firm   { legalName: String[1]; owner: model::Other[1]; } "
                        + "Class model::Person { firm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_NAME VARCHAR(50), OTHER_ID INTEGER) "
                        + "  Table T_OTHER  (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Other( T_PERSON.OTHER_ID = T_OTHER.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Broker { name: String[1]; license: String[1]; } "
                        + "Class model::Person { broker: model::Broker[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (BROKER_NAME VARCHAR(50), BROKER_LICENSE VARCHAR(50)) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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

        AppliedFunction brokerNew = (AppliedFunction) personNi.first("broker").value();
        assertEquals("new", fn(brokerNew));
        assertEquals(new PackageableElementPtr("model::Broker"), brokerNew.parameters().get(0));
        NewInstance brokerNi = (NewInstance) brokerNew.parameters().get(1);
        assertEquals("model::Broker", brokerNi.className());
        assertEquals(List.of("name", "license"),
                List.copyOf(brokerNi.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "InlineEmbedded splices the sibling Broker mapping's PMs in declaration order");

        // Sub-PM values read from the OUTER $row (sibling's mainTable matches outer).
        AppliedProperty nameRef = (AppliedProperty) toOneInner(brokerNi.first("name").value());
        assertEquals("BROKER_NAME", nameRef.property());
        assertEquals(new Variable("row"), nameRef.receiver());
    }

    @Test
    void inlineEmbeddedPM_unknownSetId_throws() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Broker { name: String[1]; } "
                        + "Class model::Person { broker: model::Broker[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, IS_ACTIVE BIT) "
                        + "  Filter ActiveFirms( T_FIRM.IS_ACTIVE = 1 ) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] @Person_Firm | [db::DB] ActiveFirms "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: map(filter(join(tableRef, ColSpec, cond), <lambda>), ^Person)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", fn(filterCall));

        AppliedFunction joinCall = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function());
        com.legend.protocol.spec.ColSpec spec =
                (com.legend.protocol.spec.ColSpec) joinCall.parameters().get(1);
        assertEquals("Person_Firm", spec.name(),
                "JoinMediated filter alias = join name (shared with any PM "
                        + "going through @Person_Firm)");

        AppliedFunction tableRef = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));

        // Filter condition: $row.Person_Firm.IS_ACTIVE == 1
        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        assertEquals(List.of(new Variable("row")), filterLambda.parameters());
        AppliedFunction eq = (AppliedFunction) sole(filterLambda.body());
        assertEquals("equal", fn(eq));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~distinct "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction distinctCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("distinct", fn(distinctCall),
                "distinct() sits between source pipeline and the terminal map()");
        assertEquals(1, distinctCall.parameters().size(),
                "distinct() with no args (zero-arg overload) over the relation");
        // engine semantics: DISTINCT over the MAPPED columns — the source
        // narrows to a select of exactly the consumed columns first (the
        // table's unmapped PK would otherwise defeat the dedup)
        AppliedFunction select = (AppliedFunction) distinctCall.parameters().get(0);
        assertEquals("select", fn(select),
                "~distinct dedups the MAPPED columns: select narrows first");
        AppliedFunction tableRef = (AppliedFunction) select.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));
    }

    @Test
    @DisplayName("P0-4: ~distinct over join reads only dedups by the slots, never the raw row")
    void distinctOverJoinReadsOnlyDedupsBySlots() {
        // audit 2026-09-15 P0-4 (proven by probe): both properties are
        // join-terminal reads; the old third branch emitted distinct(<row>)
        // over the physical row, whose unique ID made the dedup a no-op.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { firmName: String[1]; firmCity: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), CITY VARCHAR(50)) "
                        + "  Join P_F (PT.FIRM_ID = FT.ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~distinct "
                        + "    ~mainTable [db::DB] PT "
                        + "    firmName: [db::DB] @P_F | FT.NAME, "
                        + "    firmCity: [db::DB] @P_F | FT.CITY "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction distinctCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("distinct", fn(distinctCall));
        assertEquals(2, distinctCall.parameters().size(),
                "distinct BY the listed columns — never the zero-arg raw-row form");
        ColSpecArray by = (ColSpecArray) distinctCall.parameters().get(1);
        assertEquals(List.of("P_F"), by.colSpecs().stream().map(ColSpec::name).toList(),
                "the only dedup key is the join slot the two reads ride");
    }

    @Test
    @DisplayName("P0-4: ~distinct over an embedded block dedups by the block's main-table columns")
    void distinctOverEmbeddedBlockDedupsByItsColumns() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Addr { city: String[1]; } "
                        + "Class model::P { name: String[1]; addr: model::Addr[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), CITY VARCHAR(50)) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~distinct "
                        + "    ~mainTable [db::DB] PT "
                        + "    name: PT.NAME, "
                        + "    addr ( city: PT.CITY ) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction distinctCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("distinct", fn(distinctCall));
        AppliedFunction select = (AppliedFunction) distinctCall.parameters().get(0);
        assertEquals("select", fn(select), "plain reads: select narrows to the mapped columns first");
        ColSpecArray cols = (ColSpecArray) select.parameters().get(1);
        assertEquals(List.of("NAME", "CITY"), cols.colSpecs().stream().map(ColSpec::name).toList(),
                "the embedded block's column reads are main-table reads and join the dedup key");
    }

    @Test
    @DisplayName("P0-6 adjudicated: a hop declared (INNER) is ISOLATED by the engine — the parent row shape is LEFT")
    void innerHopIsIsolatedNeverAParentFilter() {
        // Audit 2026-09-15 P0-6 claimed a per-hop (INNER) "keeps parent rows
        // the engine drops". Measured against the engine's own fixtures
        // (testInnerJoinIsolationAtRoot / AtChild, testChainedInnerJoinsMerge,
        // the sqlQueryMerging family): the engine isolates a property
        // mapping's chain in the property's own subquery, so an INNER hop
        // never drops the parent — a LEFT+filter realization LOST 22 DuckDB
        // rows (docs/GATES.md, audit fix A4). The pipeline is therefore the
        // SAME with and without the annotation, on purpose.
        String shared =
                "Class model::P { orgName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, ORG_ID INTEGER) "
                        + "  Table OT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Join P_F (PT.FIRM_ID = FT.ID) "
                        + "  Join F_O (FT.ORG_ID = OT.ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~mainTable [db::DB] PT "
                        + "    orgName: [db::DB] @P_F > JOINTYPE @F_O | OT.NAME "
                        + "  } "
                        + ")";
        FunctionDefinition plain = soleSynth(normalizeViaPipeline(
                com.legend.testing.Own.model(shared.replace("JOINTYPE ", ""))));
        FunctionDefinition inner = soleSynth(normalizeViaPipeline(
                com.legend.testing.Own.model(shared.replace("JOINTYPE", "(INNER)"))));
        assertEquals(plain.body(), inner.body(),
                "an (INNER) hop is isolated in the property's own subquery: no parent-row filter");
    }

    @Test
    @DisplayName("P0-6 adjudicated: a class-typed hop declared (INNER) is the ordinary by-demand navigate")
    void innerClassTypedHopStaysByDemand() {
        // 38 corpus PMs carry this shape (`employees: (INNER) @Firm_Person`)
        // and are EXACT under the by-demand navigate: the engine's isolation
        // of the property's join keeps the parent row.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { name: String[1]; org: model::O[1]; } "
                        + "Class model::O { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, ORG_ID INTEGER) "
                        + "  Table OT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Join P_F (PT.FIRM_ID = FT.ID) "
                        + "  Join F_O (FT.ORG_ID = OT.ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::P: Relational { ~mainTable [db::DB] PT name: PT.NAME, "
                        + "    org: [db::DB] @P_F > (INNER) @F_O } "
                        + "  *model::O: Relational { ~mainTable [db::DB] OT name: OT.NAME } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        assertTrue(poisonsOf(normalized).isEmpty(), () -> "no poison: " + poisonsOf(normalized));
        FunctionDefinition fn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::P")).findFirst().orElseThrow();
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction nav = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(com.legend.builtin.Pure.Lite.LEGACY_NAVIGATE, nav.function(),
                "the class-typed (INNER) hop is the ordinary by-demand navigate");
        AppliedFunction hop1 = (AppliedFunction) nav.parameters().get(0);
        assertEquals(com.legend.builtin.Pure.Lite.JOIN_SLOT, hop1.function(),
                "no parent-row filter is added for the hop");
    }

    @Test
    @DisplayName("P0-7: a class-typed property never dedups onto a physical slot of the same name")
    void classHopMintsPastAPhysicalSlotOfTheSameName() {
        // audit 2026-09-15 P0-7: the join-terminal chain `@firm` claims the
        // physical slot "firm"; the class-typed property `firm` used to take
        // the dedup `continue` (no navigate) and the constructor bound the
        // physical sub-row where a Firm instance belongs.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { firmName: String[1]; firm: model::F[1]; } "
                        + "Class model::F { legalName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Join firm (PT.FIRM_ID = FT.ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::P: Relational { ~mainTable [db::DB] PT "
                        + "    firmName: [db::DB] @firm | FT.NAME, "
                        + "    firm: [db::DB] @firm } "
                        + "  *model::F: Relational { ~mainTable [db::DB] FT legalName: FT.NAME } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = normalized.elements().stream()
                .filter(e -> e instanceof FunctionDefinition f
                        && f.qualifiedName().equals("my::M$class$model::P"))
                .map(FunctionDefinition.class::cast).findFirst().orElseThrow();
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        java.util.function.UnaryOperator<ValueSpecification> unwrap = v ->
                v instanceof AppliedFunction af && af.function().equals(Pure.Lite.TRUST_ONE)
                        ? sole(af.parameters()) : v;
        AppliedProperty firmRead = (AppliedProperty) unwrap.apply(ni.first("firm").value());
        AppliedProperty nameRead = (AppliedProperty) unwrap.apply(ni.first("firmName").value());
        String navSlot = firmRead.property();
        String physicalSlot = ((AppliedProperty) nameRead.receiver()).property();
        assertEquals("firm", physicalSlot, "the join-terminal chain keeps the physical slot");
        assertEquals("firm_", navSlot, "the class hop is minted past it");
        // and the navigate for the class hop was emitted (not dedup'd away)
        AppliedFunction nav = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(com.legend.builtin.Pure.Lite.LEGACY_NAVIGATE, nav.function());
        assertEquals("firm_", ((ColSpec) nav.parameters().get(1)).name());
    }

    @Test
    @DisplayName("P0-3: a self-referential M2M class-typed property is legal — no false cycle")
    void selfReferentialM2mPropertyCompiles() {
        // audit 2026-09-15 P0-3: the old guard added the OWNING class to its
        // stack and the value never recursed, so `Person.manager: Person`
        // was rejected as a cycle and failed a STRICT build.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; manager: model::Person[0..1]; } "
                        + "Class model::Src { name: String[1]; manager: model::Src[0..1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Pure { ~src model::Src name: $src.name, manager: $src.manager } "
                        + ")");
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> normalizeStrict(parsed));
        assertTrue(poisonsOf(normalizeViaPipeline(parsed)).isEmpty());
    }

    @Test
    @DisplayName("P0-3: a cyclic Inline embedded splice is loud, never a StackOverflowError")
    void cyclicInlineEmbeddedIsLoud() {
        // set a splices set b which splices set a: the old class-keyed
        // guard was handed a fresh set by every caller and never fired.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { name: String[1]; b: model::B[1]; } "
                        + "Class model::B { name: String[1]; a: model::A[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T (A_NAME VARCHAR(50), B_NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::A[a]: Relational { ~mainTable [db::DB] T name: T.A_NAME, b() Inline[b] } "
                        + "  model::B[b]: Relational { ~mainTable [db::DB] T name: T.B_NAME, a() Inline[a] } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class, () -> normalizeStrict(parsed));
        assertTrue(ex.getMessage().contains("Cycle materializing Inline embedded set"),
                () -> "Expected the inline cycle to be named; got: " + ex.getMessage());
        // a MODULE build walls the mapping in the pre-pass (the route
        // collector meets the cycle first) with the reason recorded
        java.util.Map<String, String> walls = new java.util.LinkedHashMap<>();
        MappingNormalizer.normalize(parsed, ModelBuilder.from(
                new com.legend.model.ParsedModel(parsed.elements(), parsed.imports())), walls);
        String walled = String.join(" ;; ", walls.values());
        assertTrue(walled.contains("Cycle materializing Inline embedded set"),
                () -> "Expected the module build to wall the mapping with the cycle; got: " + walled);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Enum app::Status { Active, Inactive } "
                        + "Class model::Person { status: app::Status[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (STATUS VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        AppliedFunction outerIf = (AppliedFunction) toOneInner(ni.first("status").value());
        assertEquals("if", fn(outerIf),
                "EnumeratedColumn inlines as nested if(...) chain");
        assertEquals(3, outerIf.parameters().size());

        AppliedFunction outerEq = (AppliedFunction) outerIf.parameters().get(0);
        assertEquals("equal", fn(outerEq));
        AppliedProperty colRefA = (AppliedProperty) outerEq.parameters().get(0);
        assertEquals("STATUS", colRefA.property());
        assertEquals(new Variable("row"), colRefA.receiver());
        assertEquals(new com.legend.protocol.spec.CString("A"),
                outerEq.parameters().get(1),
                "First branch tests against source value 'A'");

        LambdaFunction thenA = (LambdaFunction) outerIf.parameters().get(1);
        assertEquals(List.of(), thenA.parameters());
        com.legend.protocol.spec.EnumValue activeRef =
                (com.legend.protocol.spec.EnumValue) sole(thenA.body());
        assertEquals("app::Status", activeRef.fullPath());
        assertEquals("Active", activeRef.value());

        // Inner: if(equal($row.STATUS, 'I'), {| Inactive}, {| []})
        LambdaFunction elseOuter = (LambdaFunction) outerIf.parameters().get(2);
        AppliedFunction innerIf = (AppliedFunction) sole(elseOuter.body());
        assertEquals("if", fn(innerIf));
        AppliedFunction innerEq = (AppliedFunction) innerIf.parameters().get(0);
        assertEquals(new com.legend.protocol.spec.CString("I"),
                innerEq.parameters().get(1),
                "Second branch tests against 'I'");
        LambdaFunction thenI = (LambdaFunction) innerIf.parameters().get(1);
        com.legend.protocol.spec.EnumValue inactiveRef =
                (com.legend.protocol.spec.EnumValue) sole(thenI.body());
        assertEquals("Inactive", inactiveRef.value());

        // Final else: empty collection (no match).
        LambdaFunction defaultBranch = (LambdaFunction) innerIf.parameters().get(2);
        com.legend.protocol.spec.PureCollection emptyColl =
                (com.legend.protocol.spec.PureCollection) sole(defaultBranch.body());
        assertEquals(List.of(), emptyColl.values());
    }

    @Test
    @DisplayName("localProperty_wrappingColumn_behavesLikeColumn")
    void localProperty_wrappingColumn_behavesLikeColumn() {
        // +localTag: String[1]: T_PERSON.TAG produces the same value
        // expression as a Column PM (just with a different name + declared
        // type, which is parser-level metadata).
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50), EXTRA VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
                List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));
        // localTag value: $row.EXTRA (translates as a Column would)
        AppliedProperty localTag = (AppliedProperty) ni.first("localTag").value();
        assertEquals("EXTRA", localTag.property());
        assertEquals(new Variable("row"), localTag.receiver());
    }

    @Test
    @DisplayName("localProperty_wrappingExpression_behavesLikeExpression")
    void localProperty_wrappingExpression_behavesLikeExpression() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50), A VARCHAR(50), B VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
                ni.first("computed").value(), 3);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.spec.ColSpec) nav.parameters().get(1)).name(),
                "OE fallback emits a legacyNavigate slot named after the property");

        // Condition: {s, t | $s.FIRM_ID == $t.ID}.
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction condEq = (AppliedFunction) sole(cond.body());
        assertEquals("equal", fn(condEq));
        assertEquals("FIRM_ID", propOf(condEq.parameters().get(0)));
        assertEquals("s", recvOf(condEq.parameters().get(0)));
        assertEquals("ID", propOf(condEq.parameters().get(1)));

        // Ctor: name + firm; firm = otherwise(^Firm(legalName=$row.FIRM_NAME), $row.firm).
        NewInstance ni = ctorOf(personFn);
        assertEquals(List.of("name", "firm"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "Ctor declares the OE slot ('firm'); no +propFk carrier");
        AppliedFunction otherwiseCall = (AppliedFunction) ni.first("firm").value();
        assertEquals(Pure.Lite.OTHERWISE, otherwiseCall.function(),
                "OE composes otherwise(partial ^Firm, fallback slot)");

        // First arg: partial ^Firm with ONLY the inline-cached legalName.
        AppliedFunction firmNewCall = (AppliedFunction) otherwiseCall.parameters().get(0);
        assertEquals("new", fn(firmNewCall));
        NewInstance partialFirm = (NewInstance) firmNewCall.parameters().get(1);
        assertEquals(List.of("legalName"),
                List.copyOf(partialFirm.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "Partial inline ^Firm carries ONLY the embedded (inline-cached) properties");
        AppliedProperty legalNameInline =
                (AppliedProperty) toOneInner(partialFirm.first("legalName").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm ( "
                        + "      legalName: T_PERSON.FIRM_NAME "
                        + "    ) Otherwise ([firm_set1]: [db::DB] @Person_Firm) "
                        + "  } "
                        + ")");
        // per-class fault isolation: recorded as a POISON on the compiled
        // mapping's facts, raised at use
        String reason = poisonsOf(normalizeViaPipeline(parsed)).get("my::M::model::Person");
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Firm   { org: model::Org[1]; legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org  ( T_FIRM.ORG_ID = T_ORG.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.spec.ColSpec) nav.parameters().get(1)).name());
        AppliedFunction intermediate = (AppliedFunction) nav.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, intermediate.function(),
                "multi-hop OE fallback emits the intermediate hop as a join");
        // Terminal composes otherwise(^Firm(...), $row.firm).
        AppliedFunction otherwiseCall =
                (AppliedFunction) ctorOf(personFn).first("firm").value();
        assertEquals(Pure.Lite.OTHERWISE, otherwiseCall.function(),
                "multi-hop OE still composes otherwise(partial, fallback slot)");
    }

    @Test
    @DisplayName("joinTypeAnnotation_acceptedAndProducesSameASTAsNoAnnotation")
    void joinTypeAnnotation_acceptedAndProducesSameASTAsNoAnnotation() {
        // Property-mapping joins ALWAYS lower to LEFT OUTER JOIN -- a Person
        // with no matching firm should still appear with firmName = null,
        // not drop out. So the parser's (OUTER) annotation is informational
        // (captured for engine grammar fidelity) but the normalizer's AST
        // doesn't differentiate. Two mappings differing ONLY in an (OUTER)
        // annotation produce structurally-equal synth bodies. Engine admits
        // exactly INNER | OUTER (RelationalParseTreeWalker:101) -- the
        // (LEFT) this fixture once used is engine-refused, and now refuses
        // here too (see joinTypeAnnotation_unknownTypeRefuses).
        String shared =
                "Class model::Person { orgName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID   = T_ORG.ID  ) "
                        + ") ";
        String mappingWithoutAnnotation =
                "\n###Mapping\nMapping a::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    orgName: [db::DB] @Person_Firm > @Firm_Org | T_ORG.NAME "
                        + "  } "
                        + ")";
        String mappingWithOuterAnnotation =
                "\n###Mapping\nMapping a::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    orgName: [db::DB] @Person_Firm > (OUTER) @Firm_Org | T_ORG.NAME "
                        + "  } "
                        + ")";

        FunctionDefinition fnWithout = soleSynth(normalizeViaPipeline(
                com.legend.testing.Own.model(shared + mappingWithoutAnnotation)));
        FunctionDefinition fnWith = soleSynth(normalizeViaPipeline(
                com.legend.testing.Own.model(shared + mappingWithOuterAnnotation)));

        assertEquals(fnWithout.body(), fnWith.body(),
                "(OUTER) annotation must not affect the synth function's AST -- "
                        + "join type is a Phase H concern, defaults to LEFT OUTER");

        // and the engine-refused spelling refuses HERE too, in engine's words
        String mappingWithLeftAnnotation =
                mappingWithOuterAnnotation.replace("(OUTER)", "(LEFT)");
        var ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.parser.ParseException.class,
                () -> com.legend.testing.Own.model(shared + mappingWithLeftAnnotation));
        org.junit.jupiter.api.Assertions.assertTrue(
                ex.getMessage().contains("Unsupported join type 'LEFT'"));
    }

    @Test
    void unknownJoinName_throws() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; firmIdStr: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        AppliedFunction firmIdStr = (AppliedFunction) toOneInner(ni.first("firmIdStr").value());
        assertEquals("plus", fn(firmIdStr));   // concat -> plus-chain
        AppliedProperty idRef = (AppliedProperty) unwrapToString(runHead(firmIdStr));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { countryName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON  (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM    (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG     (ID INTEGER, COUNTRY_ID INTEGER) "
                        + "  Table T_COUNTRY (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm ( T_PERSON.FIRM_ID    = T_FIRM.ID ) "
                        + "  Join Firm_Org    ( T_FIRM.ORG_ID       = T_ORG.ID  ) "
                        + "  Join Org_Country ( T_ORG.COUNTRY_ID    = T_COUNTRY.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    countryName: [db::DB] @Person_Firm > @Firm_Org > @Org_Country | T_COUNTRY.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Outer-most call is map; underneath are three joins nested in
        // emission order (hop1 innermost, hop3 outermost).
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction hop3Join = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop3Join.function());
        AppliedFunction hop2Join = (AppliedFunction) hop3Join.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop2Join.function());
        AppliedFunction hop1Join = (AppliedFunction) hop2Join.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop1Join.function());

        // Aliases: hop1, hop1__hop2, hop1__hop2__hop3 (join-name concatenation)
        assertEquals("Person_Firm",
                ((com.legend.protocol.spec.ColSpec) hop1Join.parameters().get(1)).name());
        assertEquals("Person_Firm__Firm_Org",
                ((com.legend.protocol.spec.ColSpec) hop2Join.parameters().get(1)).name());
        assertEquals("Person_Firm__Firm_Org__Org_Country",
                ((com.legend.protocol.spec.ColSpec) hop3Join.parameters().get(1)).name());

        // Hop 3 condition: $s.Person_Firm__Firm_Org.COUNTRY_ID == $t.ID
        // This is the load-bearing claim of the multi-hop algorithm: at
        // hop N, the source side reaches hop (N-1)'s target through that
        // hop's alias, NOT through the original mainTable scope.
        LambdaFunction hop3Cond = (LambdaFunction) hop3Join.parameters().get(2);
        AppliedFunction hop3Eq = (AppliedFunction) sole(hop3Cond.body());
        AppliedProperty left = (AppliedProperty) hop3Eq.parameters().get(0);
        assertEquals("COUNTRY_ID", left.property());
        AppliedProperty leftSub = (AppliedProperty) left.receiver();
        assertEquals("Person_Firm__Firm_Org", leftSub.property(),
                "Hop 3 must reach T_ORG (hop 2 target) via the hop-2 alias");
        assertEquals(new Variable("s"), leftSub.receiver());
        AppliedProperty right = (AppliedProperty) hop3Eq.parameters().get(1);
        assertEquals("ID", right.property());
        assertEquals(new Variable("t"), right.receiver());

        // Terminal: $row.countryName_h3.NAME
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        AppliedProperty countryName = (AppliedProperty) toOneInner(ni.first("countryName").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { "
                        + "  firmIdStr: String[1]; "
                        + "  fullName: String[1]; "
                        + "  firmName: String[1]; "
                        + "  age: Integer[1]; "
                        + "} "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), AGE INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "Project keys preserve source declaration order across "
                        + "mixed PM kinds");

        // firmIdStr (Column PM referencing T_FIRM, BEFORE the Join PM in
        // source) must resolve via the Join PM's alias -- the two-pass
        // design hoists ALL joins before project translation runs.
        AppliedFunction firmIdStr = (AppliedFunction) toOneInner(ni.first("firmIdStr").value());
        assertEquals("plus", fn(firmIdStr));   // concat -> plus-chain
        AppliedProperty idRef = (AppliedProperty) unwrapToString(runHead(firmIdStr));
        AppliedProperty firmAlias = (AppliedProperty) idRef.receiver();
        assertEquals("Person_Firm", firmAlias.property(),
                "Column PM declared BEFORE its required Join PM still resolves "
                        + "via the Join PM's alias (two-pass design)");

        // fullName (Expression PM, only T_PERSON refs) uses $row directly.
        List<ValueSpecification> fullName = plusChain(
                toOneInner(ni.first("fullName").value()), 3);
        AppliedProperty firstRef = (AppliedProperty) fullName.get(0);
        assertEquals("FIRST", firstRef.property());
        assertEquals(new Variable("row"), firstRef.receiver(),
                "Main-table column ref goes directly through $row");

        // age (Column PM, T_PERSON) directly $row.AGE
        AppliedProperty age = (AppliedProperty) toOneInner(ni.first("age").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, IS_ACTIVE BIT) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Filter ActiveOnly( T_PERSON.IS_ACTIVE = 1 ) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] ActiveOnly "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Outer is map; under it: filter -> join -> tableReference.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction filter = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", fn(filter),
                "filter is outermost upstream op (wraps the joins)");
        AppliedFunction joinCall = (AppliedFunction) filter.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function(),
                "join sits between tableReference and filter");
        AppliedFunction tableRef = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", fn(tableRef),
                "tableReference is innermost");
    }

    @Test
    @DisplayName("twoJoinPMsThroughSameJoin_shareSingleJoin")
    void twoJoinPMsThroughSameJoin_shareSingleJoin() {
        // Aliases are derived from the join name, not the PM name. Two
        // PMs going through the same join (Person_Firm) share ONE join
        // (aliased "Person_Firm") and read different columns from the
        // shared sub-row.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; firmId: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function());
        com.legend.protocol.spec.ColSpec spec =
                (com.legend.protocol.spec.ColSpec) joinCall.parameters().get(1);
        assertEquals("Person_Firm", spec.name(),
                "Both PMs through @Person_Firm dedupe to ONE join");

        // Directly under the join is the bare tableReference -- no
        // second join for the second PM.
        AppliedFunction underJoin = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", fn(underJoin),
                "No redundant second join; PMs share the prefix");

        // Both PMs read from the SAME sub-row.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        AppliedProperty firmName = (AppliedProperty) toOneInner(ni.first("firmName").value());
        assertEquals("LEGAL_NAME", firmName.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) firmName.receiver()).property(),
                "firmName reads $row.Person_Firm.LEGAL_NAME");

        AppliedProperty firmId = (AppliedProperty) toOneInner(ni.first("firmId").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; deptName: String[1]; combo: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, DEPT_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_DEPT   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Person_Dept( T_PERSON.DEPT_ID = T_DEPT.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                toOneInner(ni.first("combo").value()), 3);

        AppliedProperty firmRef = (AppliedProperty) combo.get(0);
        assertEquals("NAME", firmRef.property());
        AppliedProperty firmAlias = (AppliedProperty) firmRef.receiver();
        assertEquals("Person_Firm", firmAlias.property(),
                "T_FIRM resolves through firmName PM's alias");

        // Middle arg is the ' / ' string literal.
        assertEquals(new com.legend.protocol.spec.CString(" / "),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; orgName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, IS_ACTIVE BIT) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50), ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Filter ActiveOnly( T_PERSON.IS_ACTIVE = 1 ) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID   = T_ORG.ID  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] ActiveOnly "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::ActivePerson: Pure { "
                        + "    ~src model::RawPerson "
                        + "    ~filter $src.isActive == true "
                        + "    name: $src.name "
                        + "  } "
                        + ")",
                // Relational ~distinct: tableReference, distinct, map, new.
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~distinct "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")",
                // EnumerationMapping inline: if, equal, map, new.
                "Enum app::Status { Active, Inactive } "
                        + "Class model::Person { status: app::Status[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (STATUS VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
                        + "\n###Relational\nDatabase db::DB ( Table T (K VARCHAR(10), QTY INTEGER) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_FIRM   (ID INTEGER) "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org  ( T_FIRM.ORG_ID = T_ORG.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                        + "\n###Mapping\nMapping my::M ( ) "
                        + "\n###Runtime\nRuntime my::R { mappings: [my::M]; connections: [ "
                        + "ModelStore: [ json: #{ JsonModelConnection { "
                        + "class: model::Raw; url: 'data:application/json,[]'; } }# ] ]; }");

        Set<String> emitted = new TreeSet<>();
        for (String src : models) {
            NormalizedModel normalized = normalizeViaPipeline(com.legend.testing.Own.model(src));
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
                "map", "new", "tableReference", "getAll", "filter", "joinSlot",
                "legacyNavigate", "legacyAssocPredicate", "otherwise", "groupByComputedKeys",
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
                for (KeyExpression ke : ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList()) {
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
                if ("tableReference".equals(fn(af)) && af.parameters().size() == 2
                        && af.parameters().get(1) instanceof com.legend.protocol.spec.CString cs) {
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
                for (KeyExpression ke : ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList()) {
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
        return String.join(" ;; ", poisonsOf(normalizeViaPipeline(parsed)).values());
    }

    /** Every compiled mapping's stamped poisons, keyed {@code mapping::key}
     * (T4.1 step 2: the facts ride the artifact, not the index). */
    static java.util.Map<String, String> poisonsOf(NormalizedModel normalized) {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        for (var el : normalized.elements()) {
            if (el instanceof MappingDefinition md) {
                md.facts().poisons().forEach((k, v) -> out.put(md.qualifiedName() + "::" + k, v));
            }
        }
        return out;
    }

    /** A STRICT normalization: the first user-model error throws. */
    private static NormalizedModel normalizeStrict(ParsedModel parsed) {
        return MappingNormalizer.normalize(parsed, ModelBuilder.from(new com.legend.model.ParsedModel(parsed.elements(), parsed.imports())));
    }

    /** A MODULE (tolerant) normalization: user-model errors poison the
     * set instead of throwing, so the recorded reasons are observable —
     * a STRICT build rejects them (T4.1 step 6, §6's line;
     * ValidationLineTest witnesses both). */
    private static NormalizedModel normalizeViaPipeline(ParsedModel parsed) {
        return MappingNormalizer.normalize(parsed, ModelBuilder.from(new com.legend.model.ParsedModel(parsed.elements(), parsed.imports())),
                new java.util.LinkedHashMap<>());
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

    /** The pipeline SPINE of a synthesized body: the function names down
     * parameter 0 from the map terminal to the source (a view-backed set's
     * frame sits inside: {@code project(filter(tableReference))}). */
    private static List<String> spineNames(FunctionDefinition fn) {
        return spine(fn).stream().map(AppliedFunction::function).toList();
    }

    private static List<AppliedFunction> spine(FunctionDefinition fn) {
        List<AppliedFunction> out = new java.util.ArrayList<>();
        ValueSpecification cur = sole(fn.body());
        while (cur instanceof AppliedFunction af) {
            out.add(af);
            if (af.parameters().isEmpty()) {
                break;
            }
            cur = af.parameters().get(0);
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
    /** The first operand of a plus carrier ({@code plus([a, b, …])}). */
    private static ValueSpecification runHead(AppliedFunction plus) {
        assertEquals("plus", fn(plus));
        return assertInstanceOf(com.legend.protocol.spec.PureCollection.class,
                plus.parameters().get(0), "expected the plus run").values().get(0);
    }

    private static List<ValueSpecification> plusChain(ValueSpecification v, int n) {
        // the engine's n-ary carrier: plus([a, b, c]) — upstream's
        // string::plus(String[*]) (batch 5 leg 5)
        AppliedFunction af = assertInstanceOf(AppliedFunction.class, v,
                "expected a plus carrier");
        assertEquals("plus", fn(af));
        assertEquals(1, af.parameters().size());
        var run = assertInstanceOf(com.legend.protocol.spec.PureCollection.class,
                af.parameters().get(0), "expected the plus run");
        assertEquals(n, run.values().size());
        return run.values().stream().map(MappingNormalizerTest::unwrapToString).toList();
    }

    /**
     * Peels the {@code toString(...)} coercion the normalizer wraps around
     * every concat argument (SQL concat coerces; plus(String, String) needs
     * the wrap said at emission).
     */
    private static ValueSpecification unwrapToString(ValueSpecification v) {
        // Peels the toOne(...) trust wrap (multiplicity audit slice 2:
        // dyna operands are SQL-lane null-propagating; pure's plus is
        // strict [1], so the translator wraps each optional read — the
        // 'position' idiom made uniform) ...
        if (v instanceof AppliedFunction t1
                && (fn(t1).equals("toOne")
                        || t1.function().equals(Pure.Lite.TRUST_ONE))
                && t1.parameters().size() == 1) {
            v = t1.parameters().get(0);
        }
        // ... then the cast(..., @String) coercion around every concat
        // argument (SQL concat coerces via the DATABASE's VARCHAR cast —
        // audit: pure toString's ISO datetime form diverged from
        // SQL-concat semantics).
        return v instanceof AppliedFunction af && fn(af).equals("cast")
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
                "[1]-property ctor value must be trust-wrapped");
        assertEquals(Pure.Lite.TRUST_ONE, af.function(),
                "synth conformance spells the SQL-lane trustOne"
                + " (multiplicity audit slice 3 provenance split)");
        return sole(af.parameters());
    }

    /**
     * Extracts the {@code legacyNavigate} pipeline step from a synth fn
     * whose body is {@code map(legacyNavigate(...), row | ^Cls(...))}.
     * (New class-typed Join PM shape — doc §5.4.3.)
     */
    private static AppliedFunction navStep(FunctionDefinition fn) {
        AppliedFunction map = (AppliedFunction) sole(fn.body());
        assertEquals("map", fn(map), "synth body terminates in map(...)");
        AppliedFunction nav = (AppliedFunction) map.parameters().get(0);
        assertEquals(Pure.Lite.LEGACY_NAVIGATE, nav.function(),
                "class-typed Join PM emits a legacyNavigate pipeline step");
        return nav;
    }

    /** The {@code legacyNavigate} condition lambda's body (arg 3; arg 2 = target rows). */
    private static ValueSpecification navCondBody(FunctionDefinition fn) {
        AppliedFunction nav = navStep(fn);
        AppliedFunction tgtRows = (AppliedFunction) nav.parameters().get(2);
        assertEquals("tableReference", fn(tgtRows),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Address { region: String[1]; } "
                        + "Class model::Person  { address: model::Address[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (REGION VARCHAR(10), ADDR_ID INTEGER) "
                        + "  Table T_ADDR   (REGION VARCHAR(10), ID INTEGER) "
                        + "  Join Person_Addr( T_PERSON.REGION = T_ADDR.REGION and T_PERSON.ADDR_ID = T_ADDR.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("and", fn(andCall),
                "Composite-key condition is the AND of per-pair equalities, verbatim");
        AppliedFunction eq1 = (AppliedFunction) andCall.parameters().get(0);
        AppliedFunction eq2 = (AppliedFunction) andCall.parameters().get(1);
        assertEquals("equal", fn(eq1));
        assertEquals("equal", fn(eq2));
        assertEquals("REGION", propOf(eq1.parameters().get(0)));
        assertEquals("s", recvOf(eq1.parameters().get(0)), "source side reads $s");
        assertEquals("REGION", propOf(eq1.parameters().get(1)));
        assertEquals("t", recvOf(eq1.parameters().get(1)), "target side reads $t");
        assertEquals("ADDR_ID", propOf(eq2.parameters().get(0)));
        assertEquals("ID", propOf(eq2.parameters().get(1)));

        // Ctor reads $row.address (the navigate slot) — no +propFk lift.
        NewInstance ni = ctorOf(personFn);
        assertEquals(List.of("address"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "No +propFk carriers; the class-typed property reads the slot");
        assertEquals("address", propOf(toOneInner(ni.first("address").value())));
        assertEquals("row", recvOf(toOneInner(ni.first("address").value())));
    }

    @Test
    @DisplayName("joinPmCompositeKeyThreeColumns (R4.3)")
    void joinPmCompositeKeyThreeColumns() {
        // Three-conjunct composite key. Pins that the AND-walker is
        // properly recursive (the parser left-associates 'and', so this
        // produces and(and(a, b), c) — the walker must descend both
        // sides).
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { ka: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (A INTEGER, B INTEGER, C INTEGER) "
                        + "  Table T_TGT (A INTEGER, B INTEGER, C INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A and T_SRC.B = T_TGT.B and T_SRC.C = T_TGT.C ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("and", fn(andOuter));
        List<String> srcCols = new java.util.ArrayList<>();
        collectEqualitySourceCols(andOuter, srcCols);
        assertEquals(List.of("A", "B", "C"), srcCols,
                "All three conjunct source columns appear (recursive AND walk)");
        // Ctor reads $row.tgt (slot), no +propFk.
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));
    }

    /** Recursively collect source-side ($s) column names from a nested AND of equalities. */
    private static void collectEqualitySourceCols(AppliedFunction node, List<String> out) {
        if (fn(node).equals("and")) {
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (A INTEGER, B INTEGER) "
                        + "  Table T_TGT (A INTEGER, B INTEGER, ID INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A or T_SRC.B = T_TGT.B ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("or", fn(orCall),
                "Condition reconstructed with OR verbatim from the source AST");
        AppliedFunction eq1 = (AppliedFunction) orCall.parameters().get(0);
        AppliedFunction eq2 = (AppliedFunction) orCall.parameters().get(1);
        assertEquals("equal", fn(eq1));
        assertEquals("equal", fn(eq2));
        assertEquals("A", propOf(eq1.parameters().get(0)));
        assertEquals("s", recvOf(eq1.parameters().get(0)));
        assertEquals("B", propOf(eq2.parameters().get(0)));
        assertEquals("s", recvOf(eq2.parameters().get(0)));
        // Ctor reads $row.tgt (slot), no +propFk.
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));
    }

    @Test
    @DisplayName("joinPmNonEqualityConjunctLifts (predicate rewriter)")
    void joinPmNonEqualityConjunctLifts() {
        // Non-equality conjunct in an AND. The predicate rewriter
        // preserves the operator; the only requirement is side-
        // separability of column references, which is satisfied here
        // (T_SRC.* on left side of each conjunct, T_TGT.* on right).
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (A INTEGER, B INTEGER) "
                        + "  Table T_TGT (A INTEGER, B INTEGER, ID INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A and T_SRC.B < T_TGT.B ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("and", fn(andCall));
        AppliedFunction eq = (AppliedFunction) andCall.parameters().get(0);
        AppliedFunction lt = (AppliedFunction) andCall.parameters().get(1);
        assertEquals("equal", fn(eq),
                "First conjunct keeps `==` operator from source");
        assertEquals("meta::legend::lite::lessThan", lt.function(),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { active: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (TGT_ID INTEGER) "
                        + "  Table T_TGT (ID INTEGER, DELETED_AT INTEGER) "
                        + "  Join S_T( T_SRC.TGT_ID = T_TGT.ID and T_TGT.DELETED_AT is null ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(List.of("active"), List.copyOf(ctorOf(srcFn).properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));

        // Condition AND has equal($s.TGT_ID, $t.ID) then isNull($t.DELETED_AT).
        AppliedFunction andCall = (AppliedFunction) navCondBody(srcFn);
        assertEquals("and", fn(andCall));
        AppliedFunction eq = (AppliedFunction) andCall.parameters().get(0);
        assertEquals("TGT_ID", propOf(eq.parameters().get(0)));
        assertEquals("s", recvOf(eq.parameters().get(0)), "source side reads $s");
        AppliedFunction isNullCall = (AppliedFunction) andCall.parameters().get(1);
        assertEquals("isEmpty", fn(isNullCall));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (NAME VARCHAR(50)) "
                        + "  Table T_TGT (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join S_T( upper(T_SRC.NAME) = T_TGT.NAME ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));

        // Condition: equal(upper($s.NAME), $t.NAME) — the upper(...) wrapper
        // around the source column is preserved verbatim in the lambda.
        AppliedFunction eq = (AppliedFunction) navCondBody(srcFn);
        assertEquals("equal", fn(eq));
        AppliedFunction upperCall = (AppliedFunction) eq.parameters().get(0);
        assertEquals("upper", fn(upperCall),
                "Function call wrapper around source column is preserved in the condition");
        // the dyna fallback wraps its SQL-lane operand in toOne
        // (multiplicity audit slice 2) — peel it, then the column read
        AppliedProperty inner = (AppliedProperty) unwrapToString(
                upperCall.parameters().get(0));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (A INTEGER) "
                        + "  Table T_TGT (B INTEGER, ID INTEGER) "
                        + "  Join S_T( plus(T_SRC.A, T_TGT.B) = 5 ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));

        // Condition: equal(plus($s.A, $t.B), 5). Both refs translated
        // individually inside the plus(...) call — source to $s, target to $t.
        AppliedFunction eq = (AppliedFunction) navCondBody(srcFn);
        AppliedFunction plusCall = (AppliedFunction) eq.parameters().get(0);
        assertEquals("plus", fn(plusCall));
        // dyna operands carry the SQL-lane toOne wrap (audit slice 2)
        List<ValueSpecification> run = plusChain(plusCall, 2);
        AppliedProperty arg0 = (AppliedProperty) run.get(0);
        AppliedProperty arg1 = (AppliedProperty) run.get(1);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SRC (A INTEGER) "
                        + "  Table T_TGT (A INTEGER, ID INTEGER) "
                        + "  Table T_OTHER (X INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A and T_OTHER.X = 1 ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Person { name: String[1]; org: model::Org[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.spec.ColSpec) nav.parameters().get(1)).name(),
                "final-hop slot is the PM property name");

        // Upstream of legacyNavigate is the intermediate Person_Firm join.
        AppliedFunction hop1Join = (AppliedFunction) nav.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop1Join.function(),
                "intermediate hop is a clean join step");

        // Condition: {s, t | $s.Person_Firm.ORG_ID == $t.ID}.
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        assertEquals("equal", fn(eq));
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
        assertEquals(List.of("name", "org"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));
    }

    @Test
    @DisplayName("joinPmThreeHopMappedClass (R4.2)")
    void joinPmThreeHopMappedClass() {
        // Three-hop chain: division reachable via Person -> Firm -> Org -> Div.
        // Last hop's condition: T_ORG.DIV_ID == T_DIV.ID.
        // Source-side sub-row at second-to-last alias `Person_Firm__Firm_Org`.
        // +divisionFk = $row.Person_Firm__Firm_Org.DIV_ID.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Division { name: String[1]; } "
                        + "Class model::Person   { division: model::Division[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, DIV_ID INTEGER) "
                        + "  Table T_DIV    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + "  Join Org_Div    ( T_ORG.DIV_ID     = T_DIV.ID  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.spec.ColSpec) nav.parameters().get(1)).name());
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
        assertEquals(List.of("division"), List.copyOf(ctorOf(personFn).properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Person { org: model::Org[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        AppliedFunction hop1Join = (AppliedFunction) nav.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop1Join.function(),
                "intermediate hop is present as a join beneath the legacyNavigate");
        AppliedFunction base = (AppliedFunction) hop1Join.parameters().get(0);
        assertEquals("tableReference", fn(base),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { name: String[1]; } "
                        + "Class model::Person { parentFirm: model::Firm[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, PARENT_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Parent( T_FIRM.PARENT_ID = {target}.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.spec.ColSpec) nav.parameters().get(1)).name());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { displayName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, ADDR_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_ADDR   (ID INTEGER, STREET VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Person_Addr( T_PERSON.ADDR_ID = T_ADDR.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.JOIN_SLOT, outerJoin.function(),
                "Outer join wraps the second chain (Person_Addr)");
        AppliedFunction innerJoin = (AppliedFunction) outerJoin.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, innerJoin.function(),
                "Inner join wraps the first chain (Person_Firm)");
        AppliedFunction tableRef = (AppliedFunction) innerJoin.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));

        // Aliases: PM declaration order drives prelude order.
        assertEquals("Person_Firm",
                ((com.legend.protocol.spec.ColSpec) innerJoin.parameters().get(1)).name());
        assertEquals("Person_Addr",
                ((com.legend.protocol.spec.ColSpec) outerJoin.parameters().get(1)).name());

        // Project lambda: concat($row.Person_Firm.NAME, ' - ', $row.Person_Addr.STREET).
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        List<ValueSpecification> concat = plusChain(
                toOneInner(ni.first("displayName").value()), 3);

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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { line: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function());
        assertEquals("tableReference",
                ((AppliedFunction) joinCall.parameters().get(0)).function(),
                "Single-chain mixed expression hoists exactly one join");

        // Project: concat($row.NAME, '@', $row.Person_Firm.LEGAL_NAME).
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        List<ValueSpecification> concat = plusChain(
                toOneInner(ni.first("line").value()), 3);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, IS_ACTIVE INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Filter ActiveFirms( [db::DB] @Person_Firm | T_FIRM.IS_ACTIVE = 1 ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("filter", fn(filterCall));
        AppliedFunction joinCall = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function(),
                "Chain from the filter condition is hoisted BEFORE the filter");
        assertEquals("Person_Firm",
                ((com.legend.protocol.spec.ColSpec) joinCall.parameters().get(1)).name());

        // Filter condition: $row.Person_Firm.IS_ACTIVE == 1
        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        AppliedFunction eq = (AppliedFunction) sole(filterLambda.body());
        assertEquals("equal", fn(eq));
        // the hoisted-chain read is a ColumnRef at translate time — the
        // relational comparison conforms by EMISSION (toOne wrap)
        AppliedFunction isActiveToOne = (AppliedFunction) eq.parameters().get(0);
        assertEquals(Pure.Lite.TRUST_ONE, isActiveToOne.function(),
                "synth conformance spells trustOne (audit slice 3)");
        AppliedProperty isActive =
                (AppliedProperty) isActiveToOne.parameters().get(0);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { firmName: String[1]; tagLine: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function());
        AppliedFunction underJoin = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", fn(underJoin),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function(),
                "Chain inside a LocalProperty body is hoisted into the prelude");
        assertEquals("Person_Firm",
                ((com.legend.protocol.spec.ColSpec) joinCall.parameters().get(1)).name());

        // The +local field's value is the translated expression reading
        // $row.Person_Firm.LEGAL_NAME.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        KeyExpression displayKe = ni.first("displayFirm");
        assertTrue(displayKe.isLocal(),
                "displayFirm is a +local field (isLocal=true)");
        AppliedFunction concat = (AppliedFunction) displayKe.value();
        assertEquals("plus", fn(concat));   // concat -> plus-chain
        AppliedProperty subRowCol = (AppliedProperty) unwrapToString(runHead(concat));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { tagline: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER, ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    tagline: concat([db::DB] @Person_Firm > @Firm_Org | T_ORG.NAME, '!') "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline shape: map(join(join(tableRef, ~Person_Firm), ~Person_Firm__Firm_Org), ...)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction hop2Join = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop2Join.function());
        assertEquals("Person_Firm__Firm_Org",
                ((com.legend.protocol.spec.ColSpec) hop2Join.parameters().get(1)).name(),
                "Hop 2 alias = join-name concatenation");
        AppliedFunction hop1Join = (AppliedFunction) hop2Join.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, hop1Join.function());
        assertEquals("Person_Firm",
                ((com.legend.protocol.spec.ColSpec) hop1Join.parameters().get(1)).name());
        AppliedFunction tableRef = (AppliedFunction) hop1Join.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));

        // Hop 2's condition must reach T_FIRM (hop-1 target) through
        // $s.Person_Firm — the chained-source-path invariant.
        LambdaFunction hop2Cond = (LambdaFunction) hop2Join.parameters().get(2);
        AppliedFunction eq = (AppliedFunction) sole(hop2Cond.body());
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
        AppliedFunction concat = (AppliedFunction) toOneInner(ni.first("tagline").value());
        AppliedProperty terminal = (AppliedProperty) unwrapToString(runHead(concat));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Bucket { firmName: String[1]; count: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER, ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals(Pure.Lite.GROUP_BY_COMPUTED_KEYS, groupBy.function());
        AppliedFunction joinCall = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function(),
                "Chain from the groupBy key is hoisted BEFORE the groupBy");

        // Key ColSpec lambda body: $row.Person_Firm.NAME
        com.legend.protocol.spec.ColSpecArray keyArr =
                (com.legend.protocol.spec.ColSpecArray) groupBy.parameters().get(1);
        com.legend.protocol.spec.ColSpec firmKey = keyArr.colSpecs().get(0);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Position { acctNum: String[1]; gsn: String[1]; quantity: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_TRADE (ACC_NUM VARCHAR(50), GSN VARCHAR(50), QTY INTEGER) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("map", fn(mapCall));
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.GROUP_BY_COMPUTED_KEYS, groupBy.function(),
                "Pipeline must include a groupBy step when ~groupBy is declared");
        AppliedFunction tableRef = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));

        // Key ColSpecArray: exactly 2 keys, matching declaration order.
        com.legend.protocol.spec.ColSpecArray keyArr =
                (com.legend.protocol.spec.ColSpecArray) groupBy.parameters().get(1);
        assertEquals(2, keyArr.colSpecs().size(),
                "Key count matches rcm.groupBy() verbatim — no inferred keys");

        // Both keys match a non-agg PM, so they adopt the PM names
        // (acctNum, gsn) — the constructor reads $row.acctNum / $row.gsn.
        assertEquals("acctNum", keyArr.colSpecs().get(0).name());
        assertEquals("gsn",     keyArr.colSpecs().get(1).name());

        // Agg ColSpecArray: exactly 1 agg-spec for `quantity`.
        com.legend.protocol.spec.ColSpecArray aggArr =
                (com.legend.protocol.spec.ColSpecArray) groupBy.parameters().get(2);
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Position { acctNum: String[1]; gsn: String[1]; quantity: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_TRADE (ACC_NUM VARCHAR(50), PRODUCT_ID INTEGER, GSN VARCHAR(50), QTY INTEGER) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        com.legend.protocol.spec.ColSpecArray keyArr =
                (com.legend.protocol.spec.ColSpecArray) groupBy.parameters().get(1);
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
                List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "Identity-only keys are absent from the terminal ^Class(...)");
    }

    @Test
    @DisplayName("groupByAggregatePmSplitsIntoSelectorAndAggregator (R4.1)")
    void groupByAggregatePmSplitsIntoSelectorAndAggregator() {
        // sum(QTY) splits into a selector lambda (row | $row.QTY) and an
        // aggregator lambda (vals | $vals -> sum()) on the ColSpec.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { k: String[1]; total: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T (K VARCHAR(10), QTY INTEGER) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        com.legend.protocol.spec.ColSpecArray aggArr =
                (com.legend.protocol.spec.ColSpecArray) groupBy.parameters().get(2);
        com.legend.protocol.spec.ColSpec totalSpec = aggArr.colSpecs().get(0);
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
        assertEquals("sum", fn(aggCall));
    }

    @Test
    @DisplayName("groupByPerRowFormulaOutsideKeyWithheld (R4.1, stage-1 withhold)")
    void groupByPerRowFormulaOutsideKeyRejected() {
        // PM `extra: T.K + '_x'` is a per-row formula, neither an
        // aggregate nor a declared groupBy key. The engine's compiler
        // ACCEPTS such a mapping (the extends/primaryKey corpus fixtures
        // map `id` beside ~groupBy(aName)); only a query reading the
        // property fails, on the database. The normalizer WITHHOLDS the
        // property (the Join-PM rule) and keeps the set — no poison.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { k: String[1]; extra: String[1]; total: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T (K VARCHAR(10), QTY INTEGER) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~groupBy([db::DB] T.K) "
                        + "    ~mainTable [db::DB] T "
                        + "    k:     T.K, "
                        + "    extra: concat(T.K, '_x'), "
                        + "    total: sum(T.QTY) "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.isEmpty(),
                () -> "the set must compile with the orphan PM withheld; got: " + exMsg);
    }

    @Test
    @DisplayName("groupByConstructorReadsRowByPmName (R4.1)")
    void groupByConstructorReadsRowByPmName() {
        // The terminal ^Class(...) reads each property as $row.<pmName>,
        // NOT as $row.<originalColumn>. The key ColSpec was aliased to
        // the PM name in applyGroupBy precisely so this dispatch works.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { k: String[1]; total: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T (K VARCHAR(10), QTY INTEGER) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
        AppliedProperty kVal = (AppliedProperty) toOneInner(ni.first("k").value());
        assertEquals("k", kVal.property(),
                "Constructor reads the key by its aliased PM name, not the raw column");
        assertEquals(new Variable("row"), kVal.receiver());

        // total = $row.total  (the agg-spec's emitted column)
        AppliedProperty totalVal = (AppliedProperty) toOneInner(ni.first("total").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::SrcA { name: String[1]; } "
                        + "Class model::SrcB { name: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Pure { ~src model::SrcA name: $src.name } "
                        + "  *model::Person[alt]: Pure { ~src model::SrcB name: $src.name } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class,
                () -> normalizeViaPipeline(parsed));
        assertTrue(String.valueOf(ex.getMessage()).contains("model::Person"),
                () -> "Expected duplicate-mapping error to name the class; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("multiple ClassMappings"),
                () -> "Expected error to mention 'multiple ClassMappings'; got: " + ex.getMessage());
    }

    /** Two tables of two databases name no main table: legend-pure's and legend-engine's first check is the tables',
     *  then the databases' (RelationalInstanceSetImplementationProcessor, RelationalCompilerExtension), the class its
     *  name (the BE audit 3's M5, 2026-11-21: lite checked the databases first and printed the class's path). */
    @Test
    void twoTablesOfTwoDatabasesNameNoMainTable_asUpstreamChecksTablesFirst() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class m::P { a: String[1]; b: String[1]; } "
                        + "\n###Relational\nDatabase s::DB1 ( Table T1 (A VARCHAR(10)) ) "
                        + "Database s::DB2 ( Table T2 (B VARCHAR(10)) ) "
                        + "\n###Mapping\nMapping s::M ( m::P : Relational { a: [s::DB1]T1.A, b: [s::DB2]T2.B } )");
        // the main table is inferred where a query reads the class (lazy work): asked here directly
        var rcm = (com.legend.model.ClassMapping.Relational) firstMapping(parsed).classMappings().get(0);
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class, () -> MappingNormalizer.inferMainTable(rcm));
        assertTrue(ex.getMessage().contains("Can't find the main table for class 'P'. Please specify a main table"
                + " using the ~mainTable directive."), ex.getMessage());
    }

    /** One table's name under two databases that each declare it is TWO tables, so no main table, as legend-pure's
     *  processor counts table objects; one table a database includes from another is ONE table under two databases,
     *  upstream's inconsistent definitions (the BE audit 5's M4(a), 2026-11-21: lite said the second for both). */
    @Test
    void oneTablesNameUnderTwoDatabases_isTwoTablesOrOne_asUpstreamCountsTables() {
        String cls = "Class m::P { a: String[1]; b: String[1]; } ";
        String map = "\n###Mapping\nMapping s::M ( m::P : Relational { a: [s::DB1]T.A, b: [s::DB2]T.B } )";
        ParsedModel two = com.legend.testing.Own.model(cls + "\n###Relational\nDatabase s::DB1 ( Table T (A VARCHAR(10),"
                + " B VARCHAR(10)) ) Database s::DB2 ( Table T (A VARCHAR(10), B VARCHAR(10)) ) " + map);
        ParsedModel one = com.legend.testing.Own.model(cls + "\n###Relational\nDatabase s::DB1 ( Table T (A VARCHAR(10),"
                + " B VARCHAR(10)) ) Database s::DB2 ( include s::DB1 ) " + map);
        for (var c : List.of(java.util.Map.entry(two, "Please specify a main table using the ~mainTable directive."),
                java.util.Map.entry(one, "Inconsistent database definitions for the mapping"))) {
            var rcm = (com.legend.model.ClassMapping.Relational) firstMapping(c.getKey()).classMappings().get(0);
            ModelBuilder model = ModelBuilder.from(new com.legend.model.ParsedModel(c.getKey().elements(),
                    c.getKey().imports()));
            com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    com.legend.error.ModelException.class, () -> MappingNormalizer.inferMainTable(rcm, model));
            assertTrue(ex.getMessage().contains("Can't find the main table for class 'P'. " + c.getValue()),
                    ex.getMessage());
        }
    }

    /** Audit 2026-09-15 P0-1 (proven by probe): the validation verdict was
     * keyed by the ClassMapping OBJECT, and the multi-hop association
     * injection rebuilds the sets it injects into — so an invalid set with
     * a two-hop association on it lost its verdict and was BOUND anyway. */
    private static ParsedModel invalidSetWithTwoHopAssociation() {
        return com.legend.testing.Own.model(
                "Class model::P { name: String[1]; } "
                        + "Class model::A { id: Integer[1]; } "
                        + "Class model::F { legalName: String[1]; } "
                        + "Association model::PF { persons: model::P[*]; firm: model::F[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), A_ID INTEGER) "
                        + "  Table AT (ID INTEGER PRIMARY KEY, F_ID INTEGER) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Join P_A (PT.A_ID = AT.ID) "
                        + "  Join A_F (AT.F_ID = FT.ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::P: Relational { ~mainTable [db::DB] PT name: PT.NAME, nope: PT.NAME } "
                        + "  model::F: Relational { ~mainTable [db::DB] FT legalName: FT.NAME } "
                        + "  model::PF: Relational { AssociationMapping ( "
                        + "    firm: [db::DB] @P_A > @A_F, "
                        + "    persons: [db::DB] @A_F > @P_A "
                        + "  ) } "
                        + ")");
    }

    @Test
    @DisplayName("P0-1: an invalid set keeps its verdict across the multi-hop injection (module build poisons, never binds)")
    void invalidSetStaysWalledAcrossMultiHopInjection() {
        NormalizedModel normalized = normalizeViaPipeline(invalidSetWithTwoHopAssociation());
        String reasons = String.join(" ;; ", poisonsOf(normalized).values());
        assertTrue(reasons.contains("'nope'") && reasons.contains("model::P"),
                () -> "Expected model::P's recorded reason to survive the injection; got: " + reasons);
        var md = canonicalMapping(normalized, "my::M");
        assertTrue(md.classBindings().stream().noneMatch(cb -> cb.classFqn().equals("model::P")),
                "an invalid set is withheld from the binding table");
        assertTrue(md.classBindings().stream().anyMatch(cb -> cb.classFqn().equals("model::F")),
                "the valid sibling set still binds");
    }

    @Test
    @DisplayName("P0-2: a strict build rejects the FIRST invalid set in declaration order, run after run")
    void strictBuildRejectsTheFirstInvalidSetInDeclarationOrder() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::P { name: String[1]; } "
                        + "Class model::F { legalName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table PT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::P: Relational { ~mainTable [db::DB] PT name: PT.NAME, nopeP: PT.NAME } "
                        + "  model::F: Relational { ~mainTable [db::DB] FT legalName: FT.NAME, nopeF: FT.NAME } "
                        + ")");
        for (int run = 0; run < 5; run++) {
            com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    com.legend.error.ModelException.class, () -> normalizeStrict(parsed));
            assertTrue(ex.getMessage().contains("'nopeP'"),
                    () -> "Expected the first declared invalid set (model::P) every run; got: " + ex.getMessage());
        }
        // and a module build records BOTH, by set
        String reasons = String.join(" ;; ", poisonsOf(normalizeViaPipeline(parsed)).values());
        assertTrue(reasons.contains("'nopeP'") && reasons.contains("'nopeF'"),
                () -> "Expected both recorded reasons; got: " + reasons);
    }

    @Test
    @DisplayName("P2-2: root-ness is judged in the OWNING mapping's scope, so a route to an included sole set is the un-routed navigation")
    void rootnessIsJudgedInTheOwningScope() {
        // audit 2026-09-15 P2-2: the route's set was resolved THROUGH the
        // include closure while sole-ness was counted over the QUERYING
        // mapping's own sets — an included class with one unmarked set
        // counted zero, so the route classified as a pinned single instead
        // of the class's root. One owner now: ResolvedMapping.isRootOrSole.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::C { name: String[1]; } "
                        + "Class model::Firm { legalName: String[1]; members: model::C[*]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table CT (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Join F_C (FT.ID = CT.FIRM_ID) "
                        + ") "
                        + "\n###Mapping\nMapping inc::I ( "
                        + "  model::C[c1]: Relational { ~mainTable [db::DB] CT name: CT.NAME } "
                        + ") "
                        + "Mapping my::M ( include inc::I "
                        + "  *model::Firm: Relational { ~mainTable [db::DB] FT "
                        + "    legalName: FT.NAME, members[c1]: [db::DB] @F_C } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        assertTrue(poisonsOf(normalized).isEmpty(), () -> "poisons: " + poisonsOf(normalized));
        FunctionDefinition firm = normalized.elements().stream()
                .filter(e -> e instanceof FunctionDefinition f
                        && f.qualifiedName().equals("my::M$class$model::Firm"))
                .map(FunctionDefinition.class::cast).findFirst().orElseThrow();
        AppliedFunction mapCall = (AppliedFunction) sole(firm.body());
        AppliedFunction nav = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(com.legend.builtin.Pure.Lite.LEGACY_NAVIGATE, nav.function());
        assertEquals(4, nav.parameters().size(),
                "c1 is model::C's SOLE set in the mapping that DEFINES it, so the route is"
                        + " the class's root: the plain (source, slot, rows, cond) navigate,"
                        + " never a route list");
    }

    @Test
    @DisplayName("P5-4: ~groupBy stage 2 — a class-typed Join PM navigates AFTER the aggregation")
    void groupByStageTwoNavigatesAboveTheAggregation() {
        // audit 2026-09-15 P5-4: every ~groupBy fixture was a flat single
        // table with no Join PM, and the near-miss used a JoinTerminalColumn
        // (which the stage-2 loop skips) — the whole deferred-navigate block
        // could be deleted and the suite stayed green.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Rep { name: String[1]; } "
                        + "Class model::Sale { region: String[1]; total: Integer[1]; "
                        + "                    rep: model::Rep[0..1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SALE (REGION VARCHAR(50), AMOUNT INTEGER, REP_ID INTEGER) "
                        + "  Table T_REP (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Join S_R (T_SALE.REP_ID = T_REP.ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Sale: Relational { "
                        + "    ~groupBy([db::DB] T_SALE.REGION, [db::DB] T_SALE.REP_ID) "
                        + "    ~mainTable [db::DB] T_SALE "
                        + "    region: T_SALE.REGION, "
                        + "    total: sum([db::DB] T_SALE.AMOUNT), "
                        + "    rep: [db::DB] @S_R "
                        + "  } "
                        + "  *model::Rep: Relational { ~mainTable [db::DB] T_REP name: T_REP.NAME } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        assertTrue(poisonsOf(normalized).isEmpty(), () -> "poisons: " + poisonsOf(normalized));
        FunctionDefinition fn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Sale")).findFirst().orElseThrow();
        List<String> names = spineNames(fn);
        int nav = names.indexOf(com.legend.builtin.Pure.Lite.LEGACY_NAVIGATE);
        int grouped = names.indexOf(Pure.Lite.GROUP_BY_COMPUTED_KEYS);
        assertTrue(nav >= 0, () -> "the class-typed Join PM must emit a navigate; spine " + names);
        assertTrue(grouped >= 0, () -> "the ~groupBy step must survive; spine " + names);
        assertTrue(nav < grouped,
                () -> "STAGE 2: the navigate lands ABOVE the groupBy (the engine navigates a"
                        + " grouped set through the join over the GROUPED subselect); spine " + names);
    }

    @Test
    @DisplayName("P5-4: an (INNER) mapping ~filter row-explodes through a projected subselect")
    void innerMappingFilterRowExplodes() {
        // audit 2026-09-15 P5-4: the only (INNER) filter in the suite was a
        // grammar round-trip that never reached the normalizer.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm { legalName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Table PT (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER, ACTIVE INTEGER) "
                        + "  Join F_P (FT.ID = PT.FIRM_ID) "
                        + "  Filter ActivePerson ( PT.ACTIVE = 1 ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Firm: Relational { "
                        + "    ~filter [db::DB] (INNER) @F_P | [db::DB] ActivePerson "
                        + "    ~mainTable [db::DB] FT "
                        + "    legalName: FT.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        List<String> names = spineNames(fn);
        assertEquals(List.of("map", "project", "filter", com.legend.builtin.Pure.Lite.JOIN_SLOT,
                        "tableReference"),
                names,
                "the (INNER) filter swaps the main table for a subselect that joins the filter"
                        + " chain, applies the condition and projects every base column — one row"
                        + " per matching child survives (the engine's row explosion)");
        AppliedFunction project = spine(fn).get(1);
        ColSpecArray cols = (ColSpecArray) project.parameters().get(1);
        assertEquals(List.of("ID", "NAME"), cols.colSpecs().stream().map(ColSpec::name).toList(),
                "every base column of the main table, under its original name");
    }

    @Test
    @DisplayName("P5-4: an (INNER) ~filter whose condition is NULL-TOLERANT is loud")
    void innerMappingFilterWithNullTolerantConditionIsLoud() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm { legalName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table FT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Table PT (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER, ACTIVE INTEGER) "
                        + "  Join F_P (FT.ID = PT.FIRM_ID) "
                        + "  Filter MaybeActive ( isNull(PT.ACTIVE) ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Firm: Relational { "
                        + "    ~filter [db::DB] (INNER) @F_P | [db::DB] MaybeActive "
                        + "    ~mainTable [db::DB] FT "
                        + "    legalName: FT.NAME "
                        + "  } "
                        + ")");
        String reasons = poisonReasons(parsed);
        assertTrue(reasons.contains("NULL-TOLERANT"),
                () -> "a null-tolerant (INNER) condition must be loud (LEFT+WHERE would keep"
                        + " parents the engine's INNER join drops); got: " + reasons);
    }

    @Test
    @DisplayName("P5-4: a union's key threads are named <column>_<ordinal>, one per member")
    void unionKeyThreadsAreNamedByOrdinal() {
        // audit 2026-09-15 P5-4: unionKeyThreads had ZERO assertions; the
        // name appeared in test sources only inside a comment.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table P1 (PID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + "  Table P2 (QID INTEGER PRIMARY KEY, NAME VARCHAR(50)) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::Person[p1]: Relational { ~mainTable [db::DB] P1 name: P1.NAME } "
                        + "  model::Person[p2]: Relational { ~mainTable [db::DB] P2 name: P2.NAME } "
                        + "  *model::Person: Operation { meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(p1, p2) } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        var md = canonicalMapping(normalized, "my::M");
        List<com.legend.model.KeyThread> threads =
                md.facts().unionKeyThreads().get("model::Person");
        assertTrue(threads != null && !threads.isEmpty(), "the union stamps its key threads");
        assertEquals(List.of("PID_0", "QID_1"),
                threads.stream().map(com.legend.model.KeyThread::name).toList(),
                "each member's primary key threads as <column>_<member ordinal> (the engine's"
                        + " _N key-column suffix)");
        assertEquals(List.of(0, 1),
                threads.stream().map(com.legend.model.KeyThread::ordinal).toList());
        assertEquals(List.of("PID", "QID"),
                threads.stream().map(com.legend.model.KeyThread::column).toList());
    }

    @Test
    @DisplayName("M4: one property name routed under two owners is loud, never first-owner-wins")
    void routedPropertyUnderTwoOwnersIsLoud() {
        // Firm routes `employees` at the top (owner Firm) AND inside the
        // embedded `address` block (owner Address, whose class also has an
        // `employees` property). The class mapping's routes are keyed by
        // property name, so the collector cannot hold both owners: a STRICT
        // build throws the fact; a MODULE build walls the mapping with it.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person  { name: String[1]; } "
                        + "Class model::Address { street: String[1]; employees: model::Person[*]; } "
                        + "Class model::Firm    { legalName: String[1]; employees: model::Person[*];"
                        + "                        address: model::Address[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), STREET VARCHAR(50)) "
                        + "  Table T_PERSON (ID INTEGER PRIMARY KEY, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Firm_Person (T_FIRM.ID = T_PERSON.FIRM_ID) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::Person[p1]: Relational { ~mainTable [db::DB] T_PERSON name: T_PERSON.NAME } "
                        + "  model::Person[p2]: Relational { ~mainTable [db::DB] T_PERSON name: T_PERSON.NAME } "
                        + "  *model::Person: Operation { meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(p1, p2) } "
                        + "  *model::Firm: Relational { "
                        + "    ~mainTable [db::DB] T_FIRM "
                        + "    legalName: T_FIRM.NAME, "
                        + "    employees[p1]: [db::DB]@Firm_Person, "
                        + "    address ( "
                        + "      street: T_FIRM.STREET, "
                        + "      employees[p2]: [db::DB]@Firm_Person "
                        + "    ) "
                        + "  } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class, () -> normalizeStrict(parsed));
        assertTrue(ex.getMessage().contains("'employees' is routed under two owners"),
                () -> "Expected the two-owner fact; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("model::Firm") && ex.getMessage().contains("model::Address"),
                () -> "Expected both owners named; got: " + ex.getMessage());
        // a MODULE build keeps going: the mapping is walled with the reason
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> normalizeViaPipeline(parsed));
    }

    @Test
    @DisplayName("R2: same class mapped in two distinct LegacyMappingDefinitions is allowed")
    void sameClassInTwoMappingsIsAllowed() {
        // Each LegacyMappingDefinition is its own setId namespace; mapping the
        // same class from two LegacyMappingDefinitions is a legitimate setup
        // (e.g. test vs prod). The R2 index is a Set, idempotent on union.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::SrcA { name: String[1]; } "
                        + "Class model::SrcB { name: String[1]; } "
                        + "\n###Mapping\nMapping my::MA ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class a::Person { name: String[1]; } "
                        + "Class b::Person { name: String[1]; } "
                        + "Class src::RawA { name: String[1]; } "
                        + "Class src::RawB { name: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Firm   { id: Integer[1]; } "
                        + "Class model::Person { firmId: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_FIRM   (ID INTEGER) "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.TypeExpression.NameRef) assocFn.parameters().get(0).type()).name());
        assertEquals("b", assocFn.parameters().get(1).name());
        assertEquals("model::Person",
                ((com.legend.protocol.TypeExpression.NameRef) assocFn.parameters().get(1).type()).name());
        assertEquals("meta::pure::metamodel::type::Boolean",
                ((com.legend.protocol.TypeExpression.NameRef) assocFn.returnType()).name());

        // Body: legacyAssocPredicate($a, $b, #>{srcTable}#, #>{tgtTable}#,
        // {srcRow, tgtRow | cond}) — the two ends' ~mainTable rows are
        // spelled in the call so the adapter lambda's rows TYPE (no Any).
        AppliedFunction lap = (AppliedFunction) sole(assocFn.body());
        assertEquals(Pure.Lite.LEGACY_ASSOC_PREDICATE, lap.function());
        assertEquals(new Variable("a"), lap.parameters().get(0));
        assertEquals(new Variable("b"), lap.parameters().get(1));
        AppliedFunction srcRef = (AppliedFunction) lap.parameters().get(2);
        assertEquals("tableReference", fn(srcRef));
        assertEquals(new com.legend.protocol.spec.CString("T_FIRM"),
                srcRef.parameters().get(1), "src row source is classA's (Firm) ~mainTable");
        AppliedFunction tgtRef = (AppliedFunction) lap.parameters().get(3);
        assertEquals("tableReference", fn(tgtRef));
        assertEquals(new com.legend.protocol.spec.CString("T_PERSON"),
                tgtRef.parameters().get(1), "tgt row source is classB's (Person) ~mainTable");
        LambdaFunction adapter = (LambdaFunction) lap.parameters().get(4);
        assertEquals(List.of(new Variable("srcRow"), new Variable("tgtRow")),
                adapter.parameters(),
                "adapter lambda binds the two physical-row parameters");

        // cond: equal($tgtRow.FIRM_ID, $srcRow.ID). srcRow is classA's
        // (Firm) main-table row T_FIRM; tgtRow is the join's other table.
        AppliedFunction eq = (AppliedFunction) sole(adapter.body());
        assertEquals("equal", fn(eq));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Trade { acct: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_TRADE (ACC_NUM VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertEquals("map", fn(mapCall));
        AppliedFunction source = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("tableReference", fn(source),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { x: String[1]; } "
                        + "Class model::B { x: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::A: Pure { ~src model::B  x: $src.x } "
                        + "  *model::B: Pure { ~src model::A  x: $src.x } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class,
                () -> normalizeStrict(parsed));
        assertTrue(String.valueOf(ex.getMessage()).contains("Circular M2M"),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (NAME VARCHAR(50), AGE INTEGER) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50)) "
                        + "  View V_PERSON ( ~distinct  pname: T_PERSON.NAME ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name: V_PERSON.pname "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Leg 4 (feature map §5): the view is an IDENTITY-CARRYING FRAME —
        // map( distinct( project(tableReference(T_PERSON), [pname]) ), λ );
        // ~distinct lives INSIDE the frame, the PM reads the DECLARED view
        // column VERBATIM off the frame row (never the physical column).
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", fn(mapCall));
        AppliedFunction distinctCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("distinct", fn(distinctCall),
                "view-level ~distinct rides inside the frame");
        AppliedFunction frame = (AppliedFunction) distinctCall.parameters().get(0);
        assertEquals("project", fn(frame),
                "the view frames as its own projection (row-defining subselect)");
        AppliedFunction tableRef = (AppliedFunction) frame.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));
        assertEquals("T_PERSON",
                ((com.legend.protocol.spec.CString) tableRef.parameters().get(1)).value(),
                "the frame's source is the view's physical root table");

        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        AppliedProperty nameVal = (AppliedProperty) toOneInner(ni.first("name").value());
        assertEquals("pname", nameVal.property(),
                "the PM reads the DECLARED view column off the frame row");
        assertEquals(new Variable("row"), nameVal.receiver());
    }

    @Test
    @DisplayName("viewWithMultipleRootTables_throws")
    void viewWithMultipleRootTables_throws() {
        // A view whose non-join columns reference two different physical
        // tables cannot resolve to a single root; inferViewMainTable must
        // reject it loudly (engine parity: a view has one main table).
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Mix { a: String[1]; b: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_A (X VARCHAR(50)) "
                        + "  Table T_B (Y VARCHAR(50)) "
                        + "  View V_MIX ( a: T_A.X, b: T_B.Y ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
    @DisplayName("viewWithOnlyJoinBackedColumns_infersRootFromJoin")
    void viewWithOnlyJoinBackedColumns_infersRootFromJoin() {
        // Every view column navigates a join — the view's root is the
        // FIRST join's OTHER side (the table the chain departs FROM):
        // @Person_Firm connects T_PERSON to the terminal T_FIRM, so the
        // root is T_PERSON (engine inferMainTable; the corpus
        // PersonViewWithDistinct shape). The mapping normalizes cleanly.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::J { fname: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  View V_ALLJOIN ( fname: @Person_Firm | T_FIRM.LEGAL_NAME ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::J: Relational { "
                        + "    ~mainTable [db::DB] V_ALLJOIN "
                        + "    fname: V_ALLJOIN.fname "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.isEmpty(),
                () -> "Expected the join-only view to normalize (root inferred"
                        + " from the join's other side); got: " + exMsg);
    }

    @Test
    @DisplayName("viewColumnBackedByJoin_rewritesToJoinTerminalColumn")
    void viewColumnBackedByJoin_rewritesToJoinTerminalColumn() {
        // A view column whose expression is a JoinNavigation with a column
        // terminal (@Person_Firm | T_FIRM.LEGAL_NAME) rewrites a referencing
        // PM into a JoinTerminalColumn: the pipeline emits a join() step over
        // the inferred root table (T_PERSON), and the property reads the
        // terminal column off the joined sub-row.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; firmName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  View V_PERSON ( "
                        + "    pname:     T_PERSON.NAME, "
                        + "    pfirmName: @Person_Firm | T_FIRM.LEGAL_NAME "
                        + "  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name:     V_PERSON.pname, "
                        + "    firmName: V_PERSON.pfirmName "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Leg 4: the frame carries the join INSIDE its body —
        // map( project( join(tableReference(T_PERSON), ...), cols ), λ );
        // the PM reads the declared view column VERBATIM off the frame row.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", fn(mapCall));
        AppliedFunction frame = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("project", fn(frame),
                "the view frames as its own projection");
        AppliedFunction joinCall = (AppliedFunction) frame.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, joinCall.function(),
                "the join-backed view column's join lives INSIDE the frame");
        AppliedFunction tableRef = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));
        assertEquals("T_PERSON",
                ((com.legend.protocol.spec.CString) tableRef.parameters().get(1)).value(),
                "the frame's source is the physical root table");

        NewInstance ni = ctorOf(fn);
        AppliedProperty firmVal = (AppliedProperty) toOneInner(ni.first("firmName").value());
        assertEquals("pfirmName", firmVal.property(),
                "the PM reads the DECLARED view column off the frame row");
        assertEquals(new Variable("row"), firmVal.receiver());
    }

    @Test
    @DisplayName("viewFilterAndMappingFilterAndDistinct_allThreeSurvive")
    void viewFilterAndMappingFilterAndDistinct_allThreeSurvive() {
        // AUDIT_2026_07 §16 probe — REFUTED and pinned: ~distinct wraps the
        // SOURCE pipeline (pre-map row dedup), not the map terminal, so the
        // both-filters layering composes with it. Sequencing: view filter ->
        // distinct (the view's contract) -> mapping filter -> map.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; age: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), AGE INTEGER, ACTIVE INTEGER) "
                        + "  Filter ActiveFilter ( T_PERSON.ACTIVE = 1 ) "
                        + "  Filter AdultFilter  ( T_PERSON.AGE > 18 ) "
                        + "  View V_PERSON ( "
                        + "    ~filter [db::DB] ActiveFilter "
                        + "    ~distinct "
                        + "    pname: T_PERSON.NAME, "
                        + "    page:  T_PERSON.AGE "
                        + "  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] AdultFilter "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name: V_PERSON.pname, "
                        + "    age:  V_PERSON.page "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Spine (the view is the set's FRAME): map -> filter(mapping) ->
        // distinct -> project(view columns) -> filter(view) -> tableReference
        // EXACT spine (audit 2026-09-15 P5-5: a forward search accepted any
        // number of unasserted operations between the pinned points)
        assertEquals(List.of("map", "filter", "distinct", "project", "filter", "tableReference"),
                spineNames(fn),
                "the mapping ~filter applies over the deduped view rows; ~distinct dedups the"
                        + " view's rows above the view ~filter; the frame bottoms at the table");
    }

    @Test
    @DisplayName("mapping ~filter over a grouped class pipeline lands BELOW the groupBy (WHERE, not HAVING)")
    void viewFilterAndMappingFilterAndGroupBy_filterBelowAggregation() {
        // Audit 18 finding 6: the engine evaluates the view filter AND the
        // mapping filter both in WHERE position, BEFORE aggregation.
        // Layering the mapping filter over the grouped relation silently
        // turned WHERE amount > 10 into HAVING sum(amount) > 10 whenever a
        // grouped column kept the filtered physical name.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Sale { region: String[1]; amount: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_SALE (REGION VARCHAR(50), AMOUNT INTEGER, ACTIVE INTEGER) "
                        + "  Filter ActiveFilter ( T_SALE.ACTIVE = 1 ) "
                        + "  Filter BigFilter    ( T_SALE.AMOUNT > 10 ) "
                        + "  View V_SALE ( "
                        + "    ~filter [db::DB] ActiveFilter "
                        + "    vregion: T_SALE.REGION, "
                        + "    vamount: T_SALE.AMOUNT "
                        + "  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Sale: Relational { "
                        + "    ~filter [db::DB] BigFilter "
                        + "    ~groupBy([db::DB] T_SALE.REGION) "
                        + "    ~mainTable [db::DB] V_SALE "
                        + "    region: V_SALE.vregion, "
                        + "    amount: sum(T_SALE.AMOUNT) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Spine (the view is the set's FRAME): map -> groupBy ->
        // filter(mapping) -> project(view columns) -> filter(view) -> tableReference
        // EXACT spine (audit 2026-09-15 P5-5)
        assertEquals(List.of("map", Pure.Lite.GROUP_BY_COMPUTED_KEYS, "filter", "project",
                        "filter", "tableReference"),
                spineNames(fn),
                "aggregation stays the outermost source op and the mapping ~filter applies"
                        + " BEFORE it (WHERE, not HAVING); the frame bottoms at the table");
    }

    @Test
    @DisplayName("viewFilterAndMappingFilter_layerBothFilters")
    void viewFilterAndMappingFilter_layerBothFilters() {
        // A view carries its own ~filter AND the class mapping adds another
        // ~filter. Both must survive: the engine sequences the view filter
        // first, then the mapping filter, over the physical table rows. The
        // pipeline therefore nests two filter() steps over tableReference.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; age: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), AGE INTEGER, ACTIVE INTEGER) "
                        + "  Filter ActiveFilter ( T_PERSON.ACTIVE = 1 ) "
                        + "  Filter AdultFilter  ( T_PERSON.AGE > 18 ) "
                        + "  View V_PERSON ( "
                        + "    ~filter [db::DB] ActiveFilter "
                        + "    pname: T_PERSON.NAME, "
                        + "    page:  T_PERSON.AGE "
                        + "  ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] AdultFilter "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name: V_PERSON.pname, "
                        + "    age:  V_PERSON.page "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: filter(filter(tableReference(T_PERSON), <view>), <mapping>) -> map.
        // Spine (the view is the set's FRAME): map -> filter(mapping) ->
        // project(view columns) -> filter(view) -> tableReference(T_PERSON)
        // EXACT spine (audit 2026-09-15 P5-5)
        assertEquals(List.of("map", "filter", "project", "filter", "tableReference"),
                spineNames(fn),
                "the mapping ~filter layers as the outer filter step over the view's frame");
        List<AppliedFunction> spine = spine(fn);
        AppliedFunction outerFilter = spine.get(1);
        AppliedFunction innerFilter = spine.get(3);
        AppliedFunction tableRef = spine.get(spine.size() - 1);
        assertEquals("tableReference", fn(tableRef));
        assertEquals("T_PERSON",
                ((com.legend.protocol.spec.CString) tableRef.parameters().get(1)).value());

        // Sanity: the two filter conditions read different columns (AGE vs
        // ACTIVE), proving neither filter was dropped or duplicated.
        Set<String> outerCols = new TreeSet<>();
        Set<String> innerCols = new TreeSet<>();
        collectPropertyNames(outerFilter.parameters().get(1), outerCols);
        collectPropertyNames(innerFilter.parameters().get(1), innerCols);
        // under the frame the mapping filter's T_PERSON.AGE resolves to the
        // view column that carries it (page) — the engine's alias rule
        assertTrue(outerCols.contains("page"),
                () -> "outer (mapping) filter should read AGE through the view's page; read " + outerCols);
        assertTrue(innerCols.contains("ACTIVE"),
                () -> "inner (view) filter should read ACTIVE; read " + innerCols);
    }

    @Test
    @DisplayName("viewGroupBy_layersGroupByStep")
    void viewGroupBy_layersGroupByStep() {
        // A view-level ~groupBy is merged into the effective mapping and
        // emitted as a groupBy() pipeline step over the inferred root table.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::G { k: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T (K VARCHAR(10), V VARCHAR(10)) "
                        + "  View V_G ( ~groupBy([db::DB] T.K)  vk: T.K ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::G: Relational { "
                        + "    ~mainTable [db::DB] V_G "
                        + "    k: V_G.vk "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", fn(mapCall));
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals(Pure.Lite.GROUP_BY_COMPUTED_KEYS, groupBy.function(),
                "view-level ~groupBy merges into the pipeline as a groupBy() step");
        AppliedFunction tableRef = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals("tableReference", fn(tableRef));
        assertEquals("T",
                ((com.legend.protocol.spec.CString) tableRef.parameters().get(1)).value(),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { endName: String[1]; otherName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_ROOT  (MID_ID INTEGER, OTHER_ID INTEGER) "
                        + "  Table T_MID   (ID INTEGER, END_ID INTEGER) "
                        + "  Table T_END   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_OTHER (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join A    ( T_ROOT.MID_ID = T_MID.ID ) "
                        + "  Join B    ( T_MID.END_ID = T_END.ID ) "
                        + "  Join A__B ( T_ROOT.OTHER_ID = T_OTHER.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                toOneInner(ni.first("endName").value())).receiver()).property();
        String otherSlot = ((AppliedProperty) ((AppliedProperty)
                toOneInner(ni.first("otherName").value())).receiver()).property();
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Raw { name: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( ) "
                        + "\n###Runtime\nRuntime my::R { mappings: [my::M]; connections: [ "
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
        assertEquals(Pure.Lite.SOURCE_URL, srcCall.function());

        // name = to(get($row.data, 'name'), @String).
        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(lambda.body()))
                .parameters().get(1);
        AppliedFunction toCall = (AppliedFunction) toOneInner(ni.first("name").value());
        assertEquals("to", fn(toCall));
        assertEquals(2, toCall.parameters().size());

        AppliedFunction getCall = (AppliedFunction) toCall.parameters().get(0);
        assertEquals("get", fn(getCall));
        assertEquals(2, getCall.parameters().size(),
                "get is the 2-arg variant accessor get(Variant, key)");
        AppliedProperty dataRef = (AppliedProperty) getCall.parameters().get(0);
        assertEquals("data", dataRef.property());
        assertEquals(new Variable("row"), dataRef.receiver());
        assertEquals("name",
                ((com.legend.protocol.spec.CString) getCall.parameters().get(1)).value());

        // Typed cast target @String.
        com.legend.protocol.spec.TypeAnnotation.Named typeAnn =
                (com.legend.protocol.spec.TypeAnnotation.Named) toCall.parameters().get(1);
        assertEquals("String",
                ((com.legend.protocol.TypeExpression.NameRef) typeAnn.type()).name());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::City   { name: String[1]; } "
                        + "Class model::Person { x: Integer[1]; } "
                        + "Association model::Person_City { "
                        + "  person: model::Person[1]; city: model::City[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (ADDR_ID INTEGER) "
                        + "  Table T_ADDR   (ID INTEGER, CITY_ID INTEGER) "
                        + "  Table T_CITY   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Address( T_PERSON.ADDR_ID = T_ADDR.ID ) "
                        + "  Join Address_City( T_ADDR.CITY_ID = T_CITY.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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
                ((com.legend.protocol.spec.ColSpec) nav.parameters().get(1)).name(),
                "multi-hop association end emits legacyNavigate(~city)");
        AppliedFunction intermediate = (AppliedFunction) nav.parameters().get(0);
        assertEquals(Pure.Lite.JOIN_SLOT, intermediate.function(),
                "the intermediate Person_Address hop is a join() step");
        assertEquals("Person_Address",
                ((com.legend.protocol.spec.ColSpec) intermediate.parameters().get(1)).name(),
                "intermediate join slot is the first join's alias");

        // LOAD-BEARING: the final-hop condition must read the intermediate
        // ADDR row through the prior hop's sub-row alias, i.e.
        //   {s, t | $s.Person_Address.CITY_ID == $t.ID}
        // A naive predicate-style desugar could not bind $s.Person_Address;
        // this proves the injected chain carries the intermediate correctly.
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        assertEquals("equal", fn(eq));
        AppliedProperty srcCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("CITY_ID", srcCol.property(), "source side reads ADDR.CITY_ID");
        AppliedProperty srcSub = (AppliedProperty) srcCol.receiver();
        assertEquals("Person_Address", srcSub.property(),
                "final condition reaches the intermediate row through the hop-1 alias");
        assertEquals("s", ((Variable) srcSub.receiver()).name());
        assertEquals("ID", propOf(eq.parameters().get(1)),
                "target side reaches City's PK column ($t.ID)");

        // Terminal map projects city = $row.city (the navigation slot).
        ValueSpecification cityVal = ctorOf(personFn).first("city").value();
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Base { id: Integer[1]; } "
                        + "Class model::Person extends model::Base { name: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T_PERSON (ID INTEGER, NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
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
        assertTrue(ni.first("id") != null,
                "inherited property 'id' is mapped without a 'not declared' rejection");
        assertEquals(List.of("id", "name"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()));
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Employee extends model::Person { salary: Integer[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T (NAME VARCHAR(50), SAL INTEGER) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::Person[person]: Relational { "
                        + "    ~mainTable [db::DB] T  name: T.NAME "
                        + "  } "
                        + "  *model::Employee[emp] extends [person]: Relational { "
                        + "    ~mainTable [db::DB] T  salary: T.SAL "
                        + "  } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::Employee");
        // Parent PM 'name' is inherited, then child's 'salary' (declaration order).
        assertEquals(List.of("name", "salary"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "child absorbs parent PMs (parent first), then its own");
        AppliedProperty nameRead = (AppliedProperty) toOneInner(ni.first("name").value());
        assertEquals("NAME", nameRead.property(),
                "inherited 'name' still reads the parent's T.NAME column");
    }

    @Test
    @DisplayName("extends_childOverridesParentOnPropertyNameConflict")
    void extends_childOverridesParentOnPropertyNameConflict() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Employee extends model::Person { } "
                        + "\n###Relational\nDatabase db::DB ( Table T (NAME VARCHAR(50), FULL_NAME VARCHAR(50)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::Person[person]: Relational { "
                        + "    ~mainTable [db::DB] T  name: T.NAME "
                        + "  } "
                        + "  *model::Employee[emp] extends [person]: Relational { "
                        + "    ~mainTable [db::DB] T  name: T.FULL_NAME "
                        + "  } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::Employee");
        assertEquals(List.of("name"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "conflicting property appears once");
        AppliedProperty nameRead = (AppliedProperty) toOneInner(ni.first("name").value());
        assertEquals("FULL_NAME", nameRead.property(),
                "child PM wins on property-name conflict");
    }

    @Test
    @DisplayName("extends_multiLevelResolvesRecursively")
    void extends_multiLevelResolvesRecursively() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { pa: String[1]; } "
                        + "Class model::B extends model::A { pb: String[1]; } "
                        + "Class model::C extends model::B { pc: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T (PA VARCHAR(9), PB VARCHAR(9), PC VARCHAR(9)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  model::A[a]: Relational { ~mainTable [db::DB] T  pa: T.PA } "
                        + "  model::B[b] extends [a]: Relational { ~mainTable [db::DB] T  pb: T.PB } "
                        + "  *model::C[c] extends [b]: Relational { ~mainTable [db::DB] T  pc: T.PC } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::C");
        assertEquals(List.of("pa", "pb", "pc"), List.copyOf(ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::key).toList()),
                "grandparent + parent + child PMs flatten in inheritance order");
    }

    @Test
    @DisplayName("extends_unknownParentSetIdThrows")
    void extends_unknownParentSetIdThrows() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Employee { x: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( Table T (X VARCHAR(9)) ) "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Employee[emp] extends [nope]: Relational { "
                        + "    ~mainTable [db::DB] T  x: T.X "
                        + "  } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class,
                () -> normalizeStrict(parsed));
        assertTrue(String.valueOf(ex.getMessage()).contains("nope"),
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { primaryFirmName: String[1]; secondaryFirmName: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (PRIMARY_FIRM_ID INTEGER, SECONDARY_FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_PrimaryFirm  ( T_PERSON.PRIMARY_FIRM_ID   = T_FIRM.ID ) "
                        + "  Join Person_SecondaryFirm( T_PERSON.SECONDARY_FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    primaryFirmName:   [db::DB] @Person_PrimaryFirm   | T_FIRM.NAME, "
                        + "    secondaryFirmName: [db::DB] @Person_SecondaryFirm | T_FIRM.NAME "
                        + "  } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::Person");
        AppliedProperty primary = (AppliedProperty) toOneInner(ni.first("primaryFirmName").value());
        assertEquals("NAME", primary.property());
        assertEquals("Person_PrimaryFirm", ((AppliedProperty) primary.receiver()).property(),
                "primary terminal column reads from its own sub-row slot");
        AppliedProperty secondary = (AppliedProperty) toOneInner(ni.first("secondaryFirmName").value());
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
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { "
                        + "  primaryFirmName: String[1]; secondaryFirmName: String[1]; label: String[1]; } "
                        + "\n###Relational\nDatabase db::DB ( "
                        + "  Table T_PERSON (PRIMARY_FIRM_ID INTEGER, SECONDARY_FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_PrimaryFirm  ( T_PERSON.PRIMARY_FIRM_ID   = T_FIRM.ID ) "
                        + "  Join Person_SecondaryFirm( T_PERSON.SECONDARY_FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "\n###Mapping\nMapping my::M ( "
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

    // ====================================================================
    // Audit 21a — recorded M2M mappingLine heads are honored or poisoned
    // by DESIGN (never dropped, never loud-by-accident)
    // ====================================================================

    @Test
    @DisplayName("m2mLocalProperty_poisonsAccurately_evenOnNameCollision")
    void m2mLocalProperty_poisonsAccurately_evenOnNameCollision() {
        // '+name' collides with a REAL declared property. Before audit 21a
        // the local head was dropped at parse and the binding silently
        // RETARGETED the class property (the engine keeps locals distinct).
        // Now: designed poison, checked BEFORE declaration lookup.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::T { name: String[1]; } "
                        + "Class model::S { x: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::T: Pure { ~src model::S "
                        + "    +name: String[1]: $src.x "
                        + "  } "
                        + ")");
        String reason = poisonReasons(parsed);
        assertTrue(reason.contains("local mapping property"),
                () -> "Expected the designed local-property wall; got: " + reason);
    }

    @Test
    @DisplayName("m2mExplosion_poisonsAccurately_evenWhenMultiplicityWouldPass")
    void m2mExplosion_poisonsAccurately_evenWhenMultiplicityWouldPass() {
        // 'name*' over a [1]-shaped expression: before audit 21a the marker
        // was dropped and this compiled SILENTLY (the corpus site was loud
        // only via a [*]-into-[1] multiplicity accident). Now: designed
        // poison naming the real feature gap.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::T { name: String[1]; } "
                        + "Class model::S { x: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::T: Pure { ~src model::S "
                        + "    name*: $src.x "
                        + "  } "
                        + ")");
        String reason = poisonReasons(parsed);
        assertTrue(reason.contains("explosion"),
                () -> "Expected the designed explosion wall; got: " + reason);
    }

    @Test
    @DisplayName("m2mSetRoute_nonRootSet_poisons (audit-11 shape, Pure side)")
    void m2mSetRoute_nonRootSet_poisons() {
        // 'u[u2]' routes the class-typed property to a NON-root set of U.
        // Emitting an unrouted NewInstanceCast would root-route it — wrong
        // rows with zero signal (the audit-11 shape). Designed poison.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::T { u: model::U[1]; } "
                        + "Class model::U { c: String[1]; } "
                        + "Class model::S { x: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::T: Pure { ~src model::S "
                        + "    u[u2]: $src.x "
                        + "  } "
                        + "  *model::U[u1]: Pure { ~src model::S c: $src.x } "
                        + "   model::U[u2]: Pure { ~src model::S c: $src.x } "
                        + ")");
        String reason = poisonReasons(parsed);
        assertTrue(reason.contains("set-routed") && reason.contains("u2"),
                () -> "Expected the designed non-root-route wall; got: " + reason);
    }

    @Test
    @DisplayName("m2mSetRoute_soleOrRootSet_isBenign (root-routing == honoring)")
    void m2mSetRoute_soleOrRootSet_isBenign() {
        // 'u[u1]' where u1 is U's ONLY set: root-routing is identical to
        // honoring the route — no poison, the synth proceeds (the corpus's
        // trader[trader_set] shape).
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::T { u: model::U[1]; } "
                        + "Class model::U { c: String[1]; } "
                        + "Class model::S { x: String[1]; } "
                        + "\n###Mapping\nMapping my::M ( "
                        + "  *model::T: Pure { ~src model::S "
                        + "    u[u1]: $src.x "
                        + "  } "
                        + "  *model::U[u1]: Pure { ~src model::S c: $src.x } "
                        + ")");
        java.util.Map<String, String> poisons = poisonsOf(normalizeViaPipeline(parsed));
        assertTrue(poisons.isEmpty(),
                () -> "Sole-set route must be benign; poisons: " + poisons);
    }

    @Test
    void pureKindOfIsExhaustive() {
        // remediation T1.5: null previously meant BOTH "column not found"
        // and "variant unmapped" — coercion silently skipped. Every
        // variant now names a kind; null is column-not-found ONLY.
        var t = com.legend.model.RelationalDataType.class;
        for (Class<?> v : t.getPermittedSubclasses()) {
            Object inst = instantiate(v);
            org.junit.jupiter.api.Assertions.assertNotNull(
                    RelationalKinds.pureKindOf(
                            (com.legend.model.RelationalDataType) inst),
                    v.getSimpleName() + " must name a pure kind");
        }
        org.junit.jupiter.api.Assertions.assertEquals("Byte",
                RelationalKinds.pureKindOf(
                        new com.legend.model.RelationalDataType.Binary(10)));
        org.junit.jupiter.api.Assertions.assertEquals("Variant",
                RelationalKinds.pureKindOf(
                        new com.legend.model.RelationalDataType.SemiStructured()));
    }

    private static Object instantiate(Class<?> v) {
        try {
            for (var c : v.getConstructors()) {
                Object[] args = new Object[c.getParameterCount()];
                var ps = c.getParameterTypes();
                for (int i = 0; i < args.length; i++) {
                    args[i] = ps[i] == int.class ? 1
                            : ps[i] == com.legend.model.RelationalDataType.class
                                    ? new com.legend.model.RelationalDataType.Integer_()
                                    : null;
                }
                return c.newInstance(args);
            }
        } catch (Exception e) {
            throw new RuntimeException(v + ": " + e, e);
        }
        throw new IllegalStateException("no ctor: " + v);
    }
}
