package com.legend.builtin;

import com.legend.parser.element.FunctionDefinition.ParameterDefinition;
import com.legend.parser.element.Multiplicity;
import com.legend.parser.element.NativeFunctionDefinition;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Catalog-level tests for {@link Pure} &mdash; the ported Pure stdlib of
 * native function declarations.
 *
 * <p>The class-load contract is the headline guarantee: if {@link Pure}
 * loads at all, every declared signature parsed through {@code ElementParser}.
 * The tests below pin <em>shape</em> for representative natives covering
 * every non-trivial grammar form the parser must support, so a regression
 * that changes the parsed AST (rather than rejecting the source outright)
 * fails loudly.
 */
class NativeFunctionTest {

    // ---------------------------------------------------------------
    // Catalog size + uniqueness
    // ---------------------------------------------------------------

    @Test
    void catalogSizeIsPinned() {
        // Update this number deliberately when adding or removing natives.
        assertEquals(475, Pure.all().size(),
                "Pure.all() size pin: review the catalog if this changes");
    }

    @Test
    void noTwoOverloadsCollapseToSameSignatureKey() {
        // Pure overloads on (qualifiedName, parameter-type+multiplicity tuple).
        // Two constants with the same key would mean either a duplicate
        // signature in the catalog or the parser flattening two distinct
        // declarations into the same record.
        Set<String> seen = new HashSet<>();
        for (NativeFunctionDefinition def : Pure.all()) {
            String key = signatureKey(def);
            assertTrue(seen.add(key),
                    () -> "duplicate overload key in Pure catalog: " + key);
        }
    }

    // ---------------------------------------------------------------
    // Full structural pins on representative natives.
    //
    // Each test below compares against a hand-built expected
    // NativeFunctionDefinition record, which exercises record-equality
    // over every field: qualifiedName, type/multiplicity params, every
    // parameter (name+type+multiplicity), returnType, returnMultiplicity,
    // stereotypes, taggedValues. Any drift fails loudly.
    // ---------------------------------------------------------------

    @Test
    void filterRelation_pinShape() {
        // filter<T>(Relation<T>[1], Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1]
        var expected = new NativeFunctionDefinition(
                "filter",
                List.of("T"),
                List.of(),
                List.of(
                        new ParameterDefinition("rel",
                                "meta::pure::metamodel::relation::Relation<T>",
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("f",
                                "meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>",
                                Multiplicity.exactly(1))),
                "meta::pure::metamodel::relation::Relation<T>",
                Multiplicity.exactly(1),
                List.of(),
                List.of());
        assertEquals(expected, Pure.FILTER__RELATION_1__FUNCTION_1);
    }

    @Test
    void castWithMultiplicityParameter_pinShape() {
        // cast<T|m>(Any[m], T[1]): T[m]
        // The motivating case for Multiplicity.Parameter capture.
        var expected = new NativeFunctionDefinition(
                "cast",
                List.of("T"),
                List.of("m"),
                List.of(
                        new ParameterDefinition("source",
                                "meta::pure::metamodel::type::Any",
                                new Multiplicity.Parameter("m")),
                        new ParameterDefinition("type", "T", Multiplicity.exactly(1))),
                "T",
                new Multiplicity.Parameter("m"),
                List.of(),
                List.of());
        assertEquals(expected, Pure.CAST__ANY_m__T_1);
    }

    @Test
    void renameWithSchemaAlgebraAndSubsetEquality_pinShape() {
        // rename<T,Z,K,V>(
        //   Relation<T>[1],
        //   ColSpec<Z=(?:K) \u2286 T>[1],
        //   ColSpec<V=(?:K)>[1]
        // ): Relation<T-Z+V>[1]
        var expected = new NativeFunctionDefinition(
                "rename",
                List.of("T", "Z", "K", "V"),
                List.of(),
                List.of(
                        new ParameterDefinition("r",
                                "meta::pure::metamodel::relation::Relation<T>",
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("old",
                                "meta::pure::metamodel::relation::ColSpec<Z=(?:K)\u2286T>",
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("new",
                                "meta::pure::metamodel::relation::ColSpec<V=(?:K)>",
                                Multiplicity.exactly(1))),
                "meta::pure::metamodel::relation::Relation<T-Z+V>",
                Multiplicity.exactly(1),
                List.of(),
                List.of());
        assertEquals(expected, Pure.RENAME__RELATION_1__COL_SPEC_1__COL_SPEC_1);
    }

    @Test
    void ifWithEmptyArgFunctionType_pinShape() {
        // if<T>(Boolean[1], Function<{->T[*]}>[1], Function<{->T[*]}>[1]): T[*]
        // Exercises the empty-parameter-list function-type grammar `{->T[*]}`.
        var expected = new NativeFunctionDefinition(
                "if",
                List.of("T"),
                List.of(),
                List.of(
                        new ParameterDefinition("test",
                                "meta::pure::metamodel::type::Boolean",
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("then",
                                "meta::pure::metamodel::function::Function<{->T[*]}>",
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("else",
                                "meta::pure::metamodel::function::Function<{->T[*]}>",
                                Multiplicity.exactly(1))),
                "T",
                Multiplicity.zeroMany(),
                List.of(),
                List.of());
        assertEquals(expected, Pure.IF__BOOLEAN_1__FUNCTION_1__FUNCTION_1);
    }

    @Test
    void extendWithWindowAndNestedFunctionType_pinShape() {
        // extend<T,Z,W,R>(
        //   Relation<T>[1],
        //   _Window<T>[1],
        //   FuncColSpec<{Relation<T>[1], _Window<T>[1], T[1] -> Any[0..1]}, R>[1]
        // ): Relation<T+R>[1]
        //
        // Exercises:
        //   - underscore-prefixed type _Window
        //   - multi-arg function type with three inputs
        //   - FuncColSpec generic carrying two type arguments
        //   - schema algebra return type T+R
        NativeFunctionDefinition def =
                Pure.EXTEND__RELATION_1__WINDOW_1__FUNC_COL_SPEC_1;
        assertEquals("extend", def.qualifiedName());
        assertEquals(List.of("T", "Z", "W", "R"), def.typeParameters());
        assertEquals(List.of(), def.multiplicityParameters());
        assertEquals(3, def.parameters().size());
        assertEquals("meta::pure::functions::relation::_Window<T>",
                def.parameters().get(1).type());
        // Verbatim capture of the nested function type with three input arrows.
        assertEquals(
                "meta::pure::metamodel::relation::FuncColSpec<"
                        + "{meta::pure::metamodel::relation::Relation<T>[1],"
                        + "meta::pure::functions::relation::_Window<T>[1],"
                        + "T[1]->meta::pure::metamodel::type::Any[0..1]},R>",
                def.parameters().get(2).type());
        assertEquals("meta::pure::metamodel::relation::Relation<T+R>",
                def.returnType());
        assertEquals(Multiplicity.exactly(1), def.returnMultiplicity());
    }

    @Test
    void sortWithSubsetConstraintMultiplicityMany_pinShape() {
        // sort<X,T>(Relation<T>[1], SortInfo<X \u2286 T>[*]): Relation<T>[1]
        var expected = new NativeFunctionDefinition(
                "sort",
                List.of("X", "T"),
                List.of(),
                List.of(
                        new ParameterDefinition("rel",
                                "meta::pure::metamodel::relation::Relation<T>",
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("sortInfo",
                                "meta::pure::functions::relation::SortInfo<X\u2286T>",
                                Multiplicity.zeroMany())),
                "meta::pure::metamodel::relation::Relation<T>",
                Multiplicity.exactly(1),
                List.of(),
                List.of());
        assertEquals(expected, Pure.SORT__RELATION_1__SORT_INFO_MANY);
    }

    @Test
    void zeroArityNative_pinShape() {
        // generateGuid(): String[1]
        var expected = new NativeFunctionDefinition(
                "generateGuid",
                List.of(),
                List.of(),
                List.of(),
                "meta::pure::metamodel::type::String",
                Multiplicity.exactly(1),
                List.of(),
                List.of());
        assertEquals(expected, Pure.GENERATE_GUID);
    }

    // ---------------------------------------------------------------
    // Multiplicity-parameter sweep: every native that declares a |m
    // parameter must capture it as Multiplicity.Parameter, NOT silently
    // coerce to a concrete multiplicity. This was the original bug that
    // motivated the sealed Multiplicity type.
    // ---------------------------------------------------------------

    @Test
    void everyMultiplicityParameterIsCapturedStructurally() {
        for (NativeFunctionDefinition def : Pure.all()) {
            if (def.multiplicityParameters().isEmpty()) continue;
            // Find at least one Parameter multiplicity somewhere in the
            // signature; otherwise the |m declaration was orphaned and the
            // signature is dishonest.
            boolean usesParam =
                    def.returnMultiplicity() instanceof Multiplicity.Parameter
                            || def.parameters().stream()
                                    .anyMatch(p -> p.multiplicity() instanceof Multiplicity.Parameter);
            assertTrue(usesParam,
                    () -> "native '" + def.qualifiedName()
                            + "' declares multiplicityParameters="
                            + def.multiplicityParameters()
                            + " but no parameter or return references them: "
                            + def);
            // And every Parameter multiplicity name must be declared in the
            // signature's |m,n list — the parser must not invent names.
            checkParameterNamesDeclared(def);
        }
    }

    private static void checkParameterNamesDeclared(NativeFunctionDefinition def) {
        Set<String> declared = Set.copyOf(def.multiplicityParameters());
        if (def.returnMultiplicity() instanceof Multiplicity.Parameter p) {
            assertTrue(declared.contains(p.name()),
                    () -> "return multiplicity references undeclared parameter '"
                            + p.name() + "' in " + def.qualifiedName());
        }
        for (var param : def.parameters()) {
            if (param.multiplicity() instanceof Multiplicity.Parameter p) {
                assertTrue(declared.contains(p.name()),
                        () -> "parameter '" + param.name()
                                + "' multiplicity references undeclared parameter '"
                                + p.name() + "' in " + def.qualifiedName());
            }
        }
    }

    // ---------------------------------------------------------------
    // Coverage spot-checks: the headline natives we expect downstream
    // consumers (TypeChecker, lowering, checkers) to reach for. If any
    // of these go missing from the catalog, the build fails loudly here
    // rather than at first use much later.
    // ---------------------------------------------------------------

    @Test
    void headlineNativesAreAllPresent() {
        Set<String> simpleNames = Pure.all().stream()
                .map(d -> simpleName(d.qualifiedName()))
                .collect(Collectors.toSet());
        for (String required : List.of(
                "filter", "sort", "distinct", "select", "rename", "extend",
                "concatenate", "size", "groupBy", "join", "project",
                "map", "fold", "exists", "forAll", "at", "first", "last",
                "if", "match", "eval", "cast", "instanceOf",
                "getAll", "type", "letFunction",
                "plus", "minus", "times", "divide", "equal", "lessThan",
                "and", "or", "not",
                "graphFetch", "serialize",
                "generateGuid")) {
            assertTrue(simpleNames.contains(required),
                    () -> "required native '" + required
                            + "' missing from Pure catalog");
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static String simpleName(String fqn) {
        int idx = fqn.lastIndexOf("::");
        return idx < 0 ? fqn : fqn.substring(idx + 2);
    }

    private static String signatureKey(NativeFunctionDefinition def) {
        StringBuilder key = new StringBuilder(def.qualifiedName()).append('(');
        for (var p : def.parameters()) {
            key.append(p.type()).append(':').append(p.multiplicity()).append(',');
        }
        key.append(')');
        return key.toString();
    }

}
