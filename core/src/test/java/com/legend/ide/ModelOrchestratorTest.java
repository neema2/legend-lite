package com.legend.ide;

import com.legend.parser.ElementParser;
import com.legend.parser.ImportScope;
import com.legend.parser.ParseException;
import com.legend.parser.ParsedModel;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.LegacyMappingDefinition;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.Variable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ModelOrchestrator}.
 *
 * <p>Focus areas:
 * <ul>
 *   <li><strong>Cache semantics</strong> &mdash; pure memoization on an
 *       immutable input; repeated {@code resolve(fqn)} returns the same
 *       instance.</li>
 *   <li><strong>Demand isolation</strong> &mdash; resolving one FQN
 *       does not force any others; an unrelated broken element does not
 *       affect resolution of intact ones.</li>
 *   <li><strong>Equivalence with eager path</strong> &mdash;
 *       {@code resolveAll()} produces the same {@link ParsedModel} that
 *       the historical eager parser would have.</li>
 * </ul>
 */
final class ModelOrchestratorTest {

    private static final String SOURCE = """
            import my::model::*;

            Class my::Person {
              name: String[1];
              age: Integer[0..1];
            }

            Class my::Firm {
              legalName: String[1];
            }

            function my::shout(p: my::Person[1]): String[1] {
              $p.name->toUpper()
            }

            Mapping my::M (
              *my::Person: Relational {
                ~mainTable [my::DB] T_PERSON
                name: T_PERSON.NAME
              }
            )
            """;

    // ----- cache semantics ----------------------------------------------

    @Test
    void resolveTwiceReturnsSameInstance() {
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        PackageableElement first = orch.resolve("my::Person");
        PackageableElement second = orch.resolve("my::Person");
        assertSame(first, second,
                "memoised cache must return the same instance on repeated lookups");
    }

    @Test
    void resolveAllIsIdempotent() {
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        ParsedModel a = orch.resolveAll();
        ParsedModel b = orch.resolveAll();
        assertEquals(a, b);
        // The second call must hit the cache: pick any element and compare
        // by reference identity through resolve().
        assertSame(orch.resolve("my::Person"),
                a.elements().stream()
                        .filter(e -> e instanceof ClassDefinition c && c.qualifiedName().equals("my::Person"))
                        .findFirst().orElseThrow(),
                "resolveAll() must populate the same cache that resolve() reads");
    }

    // ----- demand isolation ---------------------------------------------

    @Test
    void resolveDoesNotForceUnrelatedElements() {
        // Build a source where one element has a syntactically broken body.
        // A demand-driven resolve of an INTACT element must succeed without
        // ever touching the broken one.
        String src =
                "Class my::Good { name: String[1]; }\n"
                + "Class my::Bad { this is not valid pure syntax at all }\n";
        ModelOrchestrator orch = new ModelOrchestrator(src);

        // Shallow scan succeeds; both FQNs are visible.
        assertTrue(orch.index().contains("my::Good"));
        assertTrue(orch.index().contains("my::Bad"));

        // Resolving the good one must succeed despite the bad one existing.
        PackageableElement good = orch.resolve("my::Good");
        assertInstanceOf(ClassDefinition.class, good);
        assertEquals("my::Good", ((ClassDefinition) good).qualifiedName());

        // Resolving the bad one fails, but that failure is contained.
        assertThrows(ParseException.class, () -> orch.resolve("my::Bad"));

        // After the failure, the good one is still cached and still works.
        assertSame(good, orch.resolve("my::Good"));
    }

    // ----- unknown FQN --------------------------------------------------

    @Test
    void resolveUnknownFqnThrowsUnknownFqnException() {
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        ModelOrchestrator.UnknownFqnException ex = assertThrows(
                ModelOrchestrator.UnknownFqnException.class,
                () -> orch.resolve("my::DoesNotExist"));
        assertEquals("my::DoesNotExist", ex.fqn());
        assertTrue(ex.getMessage().contains("my::DoesNotExist"),
                () -> "message should mention the missing FQN, got: " + ex.getMessage());
    }

    // ----- index access without forcing deep parse ----------------------

    @Test
    void indexExposesAllFqnsWithoutTriggeringDeepParse() {
        new ModelOrchestrator(SOURCE);
        // Even though we have a syntactically valid source, this asserts
        // the API contract: index() doesn't deep-parse.
        // We can detect "no deep parse happened" by using a source with a
        // broken body: index() must still succeed.
        String src = "Class my::Good { x: String[1]; } Class my::Bad { ??? }";
        ModelOrchestrator broken = new ModelOrchestrator(src);
        // index() returns both FQNs without throwing.
        assertEquals(2, broken.index().size());
        assertTrue(broken.declaredFqns().contains("my::Good"));
        assertTrue(broken.declaredFqns().contains("my::Bad"));
    }

    // ----- equivalence with the eager parser path -----------------------

    @Test
    void perFqnResolveAggregatesIntoIdenticalParsedModel() {
        // Resolving each FQN one at a time and then building a ParsedModel
        // by hand must produce the same elements (in source order) as
        // resolveAll().
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        ParsedModel viaAll = orch.resolveAll();

        ModelOrchestrator orch2 = new ModelOrchestrator(SOURCE);
        // Resolve in reverse declaration order \u2014 cache must remain consistent.
        var fqns = orch2.declaredFqns().stream().toList();
        for (int i = fqns.size() - 1; i >= 0; i--) {
            orch2.resolve(fqns.get(i));
        }
        ParsedModel viaIndividual = orch2.resolveAll();
        assertEquals(viaAll, viaIndividual,
                "individual resolves followed by resolveAll must match a single resolveAll");
    }

    // ----- imports ------------------------------------------------------

    @Test
    void importsAreMemoised() {
        // Repeated imports() calls must return the same instance \u2014
        // pinpoints the memoization invariant the cache promises.
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        ImportScope first = orch.imports();
        ImportScope second = orch.imports();
        assertSame(first, second,
                "imports() must memoise; the same ImportScope instance is returned");
    }

    @Test
    void importsParsedRegardlessOfDemand() {
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        // No element resolved yet; imports() must still work.
        ImportScope imports = orch.imports();
        // Exact-list equality: SOURCE declares exactly one wildcard import
        // and no typed imports. A bug that dropped imports, fabricated
        // extras, or mis-classified wildcards-vs-typed would fail here.
        assertEquals(List.of("my::model"), imports.wildcards(),
                () -> "expected single wildcard 'my::model', got " + imports.wildcards());
        assertTrue(imports.typeImports().isEmpty(),
                () -> "SOURCE has no typed imports, got " + imports.typeImports());
    }

    // ----- specific element shapes through the demand path --------------

    @Test
    void resolveFunctionParsesBody() {
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        FunctionDefinition fn = (FunctionDefinition) orch.resolve("my::shout");
        assertEquals("my::shout", fn.qualifiedName());
        // Body '$p.name->toUpper()' desugars to
        //   toUpper(AppliedProperty(Variable("p"), "name")).
        // Pinning the exact AST shape proves the demand-driven slice path
        // produces the same body the eager path would; a slice that
        // truncates or extends would yield a different (or rejected) AST.
        assertEquals(1, fn.body().size());
        AppliedFunction toUpper = assertInstanceOf(AppliedFunction.class, fn.body().get(0));
        assertEquals("toUpper", toUpper.function());
        assertEquals(1, toUpper.parameters().size());
        AppliedProperty pName = assertInstanceOf(AppliedProperty.class, toUpper.parameters().get(0));
        assertEquals("name", pName.property());
        assertEquals(new Variable("p"), pName.receiver());
    }

    @Test
    void resolveMappingThroughSliceProducesEquivalentRecord() {
        // The demand-driven path slices the Mapping's tokens out of the
        // full stream and parses just that. Result must equal what the
        // eager path produced.
        ModelOrchestrator orch = new ModelOrchestrator(SOURCE);
        LegacyMappingDefinition viaResolve = (LegacyMappingDefinition) orch.resolve("my::M");
        LegacyMappingDefinition viaEager = (LegacyMappingDefinition) ElementParser.parse(SOURCE)
                .elements().stream()
                .filter(e -> e instanceof LegacyMappingDefinition m && m.qualifiedName().equals("my::M"))
                .findFirst().orElseThrow();
        assertEquals(viaEager, viaResolve);
    }
}
