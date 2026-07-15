package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Frontend orchestrator tests — {@link Compiler#compileModel} drives the
 * implemented pipeline steps (parse → resolve-names → normalize →
 * element-compile) and produces a queryable {@link ModelContext}.
 *
 * <p>These exercise the steps in <em>composition</em>; each step's own
 * behaviour is covered by its unit suite. The point here is the wiring: the
 * orchestrator runs them in order and the synthesized mapping function from
 * Phase E surfaces through the one {@code findFunction} of the Phase F model.
 */
@DisplayName("Compiler — frontend orchestrator (steps 1-6)")
class CompilerTest {

    @Test
    @DisplayName("compileModel wires parse → resolve → normalize → element-compile")
    void compileModelWiresFrontend() {
        ModelContext ctx = Compiler.compileModel(
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } "
              + "Mapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
              + ")");

        // Phase F (element-compile): the declared class is in the typed model.
        assertTrue(ctx.findClass("model::Person").isPresent(),
                "declared class must be element-compiled into the ModelContext");

        // Phase E (normalize) + flatten: the synthesized realizing function
        // resolves through the SAME findFunction as a user function would —
        // proving normalize ran before element-compile and the synth fn was
        // carried all the way through to the typed model.
        List<TypedFunction> synth = ctx.findFunction("my::M$class$model::Person");
        assertEquals(1, synth.size(),
                "synth mapping fn must resolve via the produced ModelContext");
        assertEquals("my::M$class$model::Person", synth.get(0).qualifiedName());
    }

    @Test
    @DisplayName("compileModel resolves simple names via imports before compiling")
    void compileModelResolvesImports() {
        // Person is referenced by simple name in the property type; the
        // resolve-names step must qualify it to model::Person against the
        // wildcard import before element-compile reads the property type.
        ModelContext ctx = Compiler.compileModel(
                "import model::*; "
              + "Class model::Person { name: String[1]; } "
              + "Class model::Employee { person: Person[1]; }");
        assertTrue(ctx.findClass("model::Employee").isPresent());
        assertTrue(ctx.findClass("model::Person").isPresent());
    }

    @Test
    @DisplayName("compileModel externalizes a derived-property body to <owner>$prop$<name>")
    void compileModelExternalizesDerivedProperty() {
        ModelContext ctx = Compiler.compileModel(
                "Class model::Person { "
              + "  name: String[1]; "
              + "  fullName() { $this.name }: String[1]; "
              + "}");

        // Phase E.2: the body is lifted into a synth function discoverable via
        // the ONE findFunction, with a leading `this` receiver.
        List<TypedFunction> synth = ctx.findFunction("model::Person$prop$fullName");
        assertEquals(1, synth.size(),
                "derived-property body must externalize to a synth fn");
        TypedFunction fn = synth.get(0);
        assertEquals("model::Person$prop$fullName", fn.qualifiedName());
        assertEquals("this", fn.parameters().get(0).name(),
                "synth fn's leading param is the `this` receiver");

        // Phase F: the class keeps the derived property's signature and
        // references the SAME synth FQN by the <owner>$prop$<name> convention.
        Property p = ctx.findProperty("model::Person", "fullName").orElseThrow();
        Property.Derived d = assertInstanceOf(Property.Derived.class, p);
        assertEquals("model::Person$prop$fullName", d.bodyFunctionFqn());
    }

    @Test
    @DisplayName("compileModel carries derived-property params after `this` in the synth fn")
    void compileModelExternalizesParameterizedDerivedProperty() {
        ModelContext ctx = Compiler.compileModel(
                "Class model::Account { "
              + "  balance: Float[1]; "
              + "  withTax(rate: Float[1]) { $this.balance * $rate }: Float[1]; "
              + "}");

        TypedFunction fn = ctx.findFunction("model::Account$prop$withTax").get(0);
        assertEquals(List.of("this", "rate"),
                fn.parameters().stream().map(TypedParameter::name).toList(),
                "synth fn params: leading `this` receiver, then the declared params");
    }

    @Test
    @DisplayName("compileModel rejects null model")
    void compileModelRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> Compiler.compileModel((String) null));
    }
}
