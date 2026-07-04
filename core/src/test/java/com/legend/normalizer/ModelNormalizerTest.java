package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.NameResolver;
import com.legend.parser.ElementParser;
import com.legend.parser.Multiplicity;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.SynthHat;
import com.legend.parser.element.ServiceDefinition;
import com.legend.parser.spec.LambdaFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase E composite normalizer tests, focused on E.2 (derived-property
 * lifting). Each case runs the real prod prefix
 * {@code parse → NameResolver.resolve → ModelNormalizer.normalize} so the
 * normalizer sees the same FQN-resolved input the orchestrator feeds it.
 *
 * <p>Behaviour of the other sub-slices (E.1 mappings) is covered by
 * {@code MappingNormalizerTest}; here we assert the E.2 contract
 * ({@code docs/CLEAN_SHEET_INVERSION.md} &sect;1): a derived property's body
 * is lifted into a {@code <owner>$prop$<name>} function appended to the
 * {@link NormalizedModel} element list as an <em>ordinary element</em>, while
 * the class's declarations stay intact (additive at the model level) and
 * ownership is recoverable only through provenance
 * ({@link NormalizedModel#liftedByOwner()}).
 */
@DisplayName("ModelNormalizer — Phase E.2 derived properties + E.3 constraints (lifted to element list)")
class ModelNormalizerTest {

    private static NormalizedModel normalize(String source) {
        return ModelNormalizer.normalize(NameResolver.resolve(ElementParser.parse(source)));
    }

    /**
     * Find a lifted function by FQN among the top-level elements — the ONLY
     * place Phase E puts behavior functions (no owner-stored copies exist).
     */
    private static FunctionDefinition function(NormalizedModel m, String fqn) {
        for (PackageableElement el : m.elements()) {
            if (el instanceof FunctionDefinition fd && fd.qualifiedName().equals(fqn)) {
                return fd;
            }
        }
        throw new AssertionError("no lifted function '" + fqn + "' in normalized model");
    }

    private static ClassDefinition clazz(NormalizedModel m, String fqn) {
        for (PackageableElement el : m.elements()) {
            if (el instanceof ClassDefinition cd && cd.qualifiedName().equals(fqn)) {
                return cd;
            }
        }
        throw new AssertionError("no ClassDefinition '" + fqn + "' in normalized model");
    }

    private static ServiceDefinition service(NormalizedModel m, String fqn) {
        for (PackageableElement el : m.elements()) {
            if (el instanceof ServiceDefinition sd && sd.qualifiedName().equals(fqn)) {
                return sd;
            }
        }
        throw new AssertionError("no ServiceDefinition '" + fqn + "' in normalized model");
    }

    @Test
    @DisplayName("derived-property body lifts into a <owner>$prop$<name> function")
    void derivedPropertyBodyIsExternalized() {
        NormalizedModel m = normalize(
                "Class model::Person { "
              + "  name: String[1]; "
              + "  fullName() { $this.name }: String[1]; "
              + "}");

        // Lifted as an ordinary top-level element; ownership lives only in
        // the derived liftedByOwner() index, never on the parser record.
        assertEquals(List.of("model::Person$prop$fullName"),
                m.liftedByOwner().get("model::Person"),
                "liftedByOwner derives the class's lifted function from provenance");

        FunctionDefinition fn = function(m, "model::Person$prop$fullName");
        // Receiver: a `this` parameter typed as the owning class.
        assertEquals("this", fn.parameters().get(0).name(),
                "lifted fn leads with the `this` receiver");
        assertEquals("model::Person",
                ((TypeExpression.NameRef) fn.parameters().get(0).type()).name(),
                "the `this` receiver is typed as the owning class");
        assertEquals(Multiplicity.Concrete.PURE_ONE, fn.parameters().get(0).multiplicity());
        assertEquals("meta::pure::metamodel::type::String",
                ((TypeExpression.NameRef) fn.returnType()).name(),
                "return type carried verbatim from the derived property (FQN-resolved)");

        // The lifted body IS the verbatim parsed derived-property expression —
        // same AST node, not a copy or reconstruction. This is the load-bearing
        // property of lifting: the body actually moves through faithfully.
        ClassDefinition cd = clazz(m, "model::Person");
        assertEquals(1, fn.body().size());
        assertSame(cd.derivedProperties().get(0).expression().get(0), fn.body().get(0),
                "lifted body is the exact parsed `$this.name` AST, not a rebuilt one");

        // Provenance: the lifted fn remembers the user-facing site it came from.
        assertTrue(fn.isSynthesized());
        assertEquals(SynthHat.PROP, fn.synthesizedFrom().hat());
        assertEquals("derived property 'fullName' of model::Person",
                fn.synthesizedFrom().describe(),
                "diagnostics can speak in user terms, not the desugared $-FQN");
    }

    @Test
    @DisplayName("E.2 is additive: the class keeps its derived property AND its body intact")
    void derivedPropertyClassLeftUntouched() {
        NormalizedModel m = normalize(
                "Class model::Person { "
              + "  name: String[1]; "
              + "  fullName() { $this.name }: String[1]; "
              + "}");

        ClassDefinition cd = clazz(m, "model::Person");
        assertEquals(1, cd.derivedProperties().size(), "derived property retained on the class");
        ClassDefinition.DerivedPropertyDefinition dp = cd.derivedProperties().get(0);
        assertEquals("fullName", dp.name());
        assertFalse(dp.expression().isEmpty(),
                "additive normalization leaves the parsed class a faithful source image "
              + "— the inline body stays put; the lifted function is an ADDITION, not a move");
    }

    @Test
    @DisplayName("declared params are carried after the `this` receiver")
    void parameterizedDerivedPropertyParamsCarried() {
        NormalizedModel m = normalize(
                "Class model::Account { "
              + "  balance: Float[1]; "
              + "  withTax(rate: Float[1]) { $this.balance * $rate }: Float[1]; "
              + "}");

        FunctionDefinition fn = function(m, "model::Account$prop$withTax");
        assertEquals(2, fn.parameters().size());
        // Receiver first, typed as the owner.
        assertEquals("this", fn.parameters().get(0).name());
        assertEquals("model::Account",
                ((TypeExpression.NameRef) fn.parameters().get(0).type()).name());
        // Declared param after it, carrying its full type + multiplicity (rate: Float[1]).
        assertEquals("rate", fn.parameters().get(1).name());
        assertEquals("meta::pure::metamodel::type::Float",
                ((TypeExpression.NameRef) fn.parameters().get(1).type()).name(),
                "declared param type carried verbatim, not dropped");
        assertEquals(Multiplicity.Concrete.PURE_ONE, fn.parameters().get(1).multiplicity());
    }

    @Test
    @DisplayName("a model with no derived properties gains no $prop$ function")
    void noDerivedPropertiesAddsNothing() {
        NormalizedModel m = normalize("Class model::Person { name: String[1]; }");
        assertFalse(m.liftedByOwner().containsKey("model::Person"),
                "no derived property => nothing lifted from the class");
        for (PackageableElement el : m.elements()) {
            assertFalse(el instanceof FunctionDefinition fd && fd.isSynthesized(),
                    "no body sites => no lifted functions anywhere in the model");
        }
    }

    @Test
    @DisplayName("M7 Door 4: a function-ref derived property is NOT lifted; Phase F points at the user function")
    void doorFourDerivedPropertyNotLiftedAndPhaseFUsesUserFqn() {
        NormalizedModel m = normalize(
                "Class model::Person { "
              + "  firstName: String[1]; "
              + "  fullName() { model::funcs::fullName }: String[1]; "
              + "} "
              + "function model::funcs::fullName(p: model::Person[1]): String[1] "
              + "  { $p.firstName }");

        // Nothing was lifted for this class — the user wrote the realizing fn.
        assertFalse(m.liftedByOwner().containsKey("model::Person"),
                "a Door-4 binding lifts nothing");
        for (PackageableElement el : m.elements()) {
            assertFalse(el instanceof FunctionDefinition fd
                            && fd.qualifiedName().equals("model::Person$prop$fullName"),
                    "no $prop$ function is synthesized for a function-ref binding");
        }

        // Phase F: the typed derived property's body FQN is the USER function,
        // not the (non-existent) lifted $prop$ FQN.
        var ctx = com.legend.compiler.element.PureModelContext.from(m);
        var person = ctx.findClass("model::Person").orElseThrow();
        var fullName = person.properties().stream()
                .filter(p -> p instanceof com.legend.compiler.element.Property.Derived d
                        && d.name().equals("fullName"))
                .map(p -> (com.legend.compiler.element.Property.Derived) p)
                .findFirst().orElseThrow();
        assertEquals("model::funcs::fullName", fullName.bodyFunctionFqn(),
                "Phase F points the derived property at the bound user function");
    }

    @Test
    @DisplayName("M7: a sugar derived property still lifts to $prop$ and Phase F points there")
    void sugarDerivedPropertyStillLiftsAndPhaseFRegenerates() {
        NormalizedModel m = normalize(
                "Class model::Person { name: String[1]; "
              + "  greeting() { $this.name }: String[1]; }");
        assertEquals(List.of("model::Person$prop$greeting"),
                m.liftedByOwner().get("model::Person"));
        var ctx = com.legend.compiler.element.PureModelContext.from(m);
        var greeting = ctx.findClass("model::Person").orElseThrow().properties().stream()
                .filter(p -> p instanceof com.legend.compiler.element.Property.Derived d
                        && d.name().equals("greeting"))
                .map(p -> (com.legend.compiler.element.Property.Derived) p)
                .findFirst().orElseThrow();
        assertEquals("model::Person$prop$greeting", greeting.bodyFunctionFqn());
    }

    // ================================================================
    // E.3 — constraints
    // ================================================================

    @Test
    @DisplayName("E.3: a constraint lifts into a <owner>$constraint$<name>(this):Boolean[1] predicate")
    void constraintExternalizedToBooleanPredicate() {
        NormalizedModel m = normalize(
                "Class model::Person [adult: $this.age >= 18] { age: Integer[1]; }");

        // Lifted top-level; the class keeps its declaration (additive).
        ClassDefinition cd = clazz(m, "model::Person");
        assertEquals(List.of("model::Person$constraint$adult"),
                m.liftedByOwner().get("model::Person"));
        assertEquals(1, cd.constraints().size(),
                "constraint declaration kept on the class (additive)");

        FunctionDefinition fn = function(m, "model::Person$constraint$adult");
        assertEquals(1, fn.parameters().size());
        assertEquals("this", fn.parameters().get(0).name(),
                "constraint is a predicate of `this`");
        assertTrue(((com.legend.parser.TypeExpression.NameRef) fn.returnType()).name()
                        .endsWith("::Boolean"),
                "return type is the Boolean primitive FQN so Phase F can classify it");
        assertEquals(Multiplicity.Concrete.PURE_ONE, fn.returnMultiplicity());
        assertEquals("model::Person",
                ((TypeExpression.NameRef) fn.parameters().get(0).type()).name(),
                "the `this` receiver is typed as the owning class");

        // The lifted body IS the verbatim parsed constraint predicate.
        assertEquals(1, fn.body().size());
        assertSame(cd.constraints().get(0).expression(), fn.body().get(0),
                "lifted body is the exact parsed `$this.age >= 18` AST");

        assertEquals(SynthHat.CONSTRAINT, fn.synthesizedFrom().hat());
        assertEquals("constraint 'adult' of model::Person", fn.synthesizedFrom().describe());
    }

    @Test
    @DisplayName("E.3 accumulates onto E.2: a class with a derived property AND a constraint lifts both fns")
    void derivedAndConstraintAccumulateOnSharedOwner() {
        NormalizedModel m = normalize(
                "Class model::Person [adult: $this.age >= 18] { "
              + "  name: String[1]; "
              + "  age: Integer[1]; "
              + "  greeting() { $this.name }: String[1]; "
              + "}");

        assertEquals(2, m.liftedByOwner().get("model::Person").size(),
                "shared owner groups: liftedByOwner derives both hats' functions");
        // Both are reachable.
        assertEquals("greeting", function(m, "model::Person$prop$greeting")
                .synthesizedFrom().memberName());
        assertEquals("adult", function(m, "model::Person$constraint$adult")
                .synthesizedFrom().memberName());
    }

    @Test
    @DisplayName("E.3: multiple constraints each get a predicate, declaration order preserved")
    void multipleConstraints() {
        NormalizedModel m = normalize(
                "Class model::Person [a: $this.x > 0, b: $this.y < 10] "
              + "{ x: Integer[1]; y: Integer[1]; }");

        assertEquals(List.of("model::Person$constraint$a", "model::Person$constraint$b"),
                m.liftedByOwner().get("model::Person"),
                "declaration order preserved in the derived index");
    }

    // ================================================================
    // E.4 — service queries
    // ================================================================

    @Test
    @DisplayName("E.4: a service query lifts into a <svc>$query():Any[*] function")
    void serviceQueryExternalized() {
        NormalizedModel m = normalize(
                "Class model::Person { name: String[1]; } "
              + "Service my::api::GetPeople { "
              + "  pattern: '/people'; "
              + "  execution: Single { query: |model::Person.all(); mapping: my::M; runtime: my::R; } "
              + "}");

        ServiceDefinition sd = service(m, "my::api::GetPeople");
        assertEquals(List.of("my::api::GetPeople$query"),
                m.liftedByOwner().get("my::api::GetPeople"),
                "query function lifted, ownership derived from provenance");

        FunctionDefinition fn = function(m, "my::api::GetPeople$query");
        assertTrue(fn.parameters().isEmpty(), "bare |query is zero-parameter");
        assertEquals("meta::pure::metamodel::type::Any",
                ((TypeExpression.NameRef) fn.returnType()).name(),
                "declared return is Any (concrete type is a Phase-G concern)");
        assertEquals(Multiplicity.Concrete.ZERO_MANY, fn.returnMultiplicity());

        // The lifted body IS the parsed query expression itself (bare |query form).
        assertEquals(1, fn.body().size());
        assertSame(sd.functionBody(), fn.body().get(0),
                "lifted body is the exact parsed query AST");
        assertEquals(SynthHat.QUERY, fn.synthesizedFrom().hat());
        assertEquals("service query of my::api::GetPeople", fn.synthesizedFrom().describe());
    }

    @Test
    @DisplayName("M7.5 Door 4: a bare-FQN service query binds to a user function and lifts nothing")
    void doorFourServiceQueryNotLifted() {
        NormalizedModel m = normalize(
                "Class model::Person { name: String[1]; } "
              + "function my::funcs::peopleQuery(): model::Person[*] { model::Person.all() } "
              + "Service my::api::GetPeople { "
              + "  pattern: '/people'; "
              + "  execution: Single { query: my::funcs::peopleQuery; mapping: my::M; runtime: my::R; } "
              + "}");
        // The service binds to the user function — no $query lift.
        assertFalse(m.liftedByOwner().containsKey("my::api::GetPeople"),
                "a Door-4 service query lifts nothing");
        for (PackageableElement el : m.elements()) {
            assertFalse(el instanceof FunctionDefinition fd
                            && fd.qualifiedName().equals("my::api::GetPeople$query"),
                    "no $query function is synthesized for a function-ref query");
        }
    }

    @Test
    @DisplayName("E.4: a typed-lambda query carries its parameters into the lifted function")
    void serviceQueryCarriesTypedParams() {
        NormalizedModel m = normalize(
                "Class model::Person { name: String[1]; } "
              + "Service my::api::ByName { "
              + "  pattern: '/person/{n}'; "
              + "  execution: Single { "
              + "    query: {n: String[1] | model::Person.all()->filter(p | $p.name == $n)}; "
              + "    mapping: my::M; runtime: my::R; "
              + "  } "
              + "}");

        FunctionDefinition fn = function(m, "my::api::ByName$query");
        assertEquals(1, fn.parameters().size(),
                "typed lambda params become the query function's params");
        assertEquals("n", fn.parameters().get(0).name());
        assertEquals("meta::pure::metamodel::type::String",
                ((TypeExpression.NameRef) fn.parameters().get(0).type()).name());
        assertEquals(Multiplicity.Concrete.PURE_ONE, fn.parameters().get(0).multiplicity());

        // The body is UNWRAPPED from the lambda: it is the lambda's body
        // (the filter expression), not the LambdaFunction itself.
        LambdaFunction lambda = (LambdaFunction) service(m, "my::api::ByName").functionBody();
        assertEquals(lambda.body().size(), fn.body().size());
        assertSame(lambda.body().get(0), fn.body().get(0),
                "query body is the lambda's inner body, unwrapped, not the lambda node");
    }

    // ================================================================
    // Integration: the one findFunction index (ordinary-element ingest)
    // ================================================================

    @Test
    @DisplayName("all three hats land in the SINGLE ModelBuilder.findFunction index")
    void synthFunctionsDiscoverableViaFindFunction() {
        // Lifting only matters if Phase F's ordinary function ingest actually
        // surfaces every lifted function in the one lookup — no special pass.
        // Exercise the real prod path (not the by-hand mirror in function()).
        NormalizedModel m = normalize(
                "Class model::Person [adult: $this.age >= 18] { "
              + "  age: Integer[1]; "
              + "  next() { $this.age } : Integer[1]; "
              + "} "
              + "Service my::S { pattern: '/x'; "
              + "  execution: Single { query: |model::Person.all(); mapping: my::M; runtime: my::R; } }");

        ModelBuilder mb = ModelBuilder.from(m);
        assertEquals(1, mb.findFunction("model::Person$prop$next").size(),
                "derived-property fn reachable through findFunction");
        assertEquals(1, mb.findFunction("model::Person$constraint$adult").size(),
                "constraint fn reachable through findFunction");
        assertEquals(1, mb.findFunction("my::S$query").size(),
                "service-query fn reachable through findFunction");
        assertTrue(mb.findFunction("model::Person$prop$missing").isEmpty(),
                "a non-existent synth FQN resolves to nothing (no fallback)");
    }
}
