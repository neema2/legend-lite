package com.legend.builtin;

import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.FunctionDefinition.ParameterDefinition;
import com.legend.parser.Multiplicity;
import com.legend.parser.element.NativeFunctionDefinition;
import com.legend.parser.TypeExpression;
import com.legend.parser.TypeExpression.Column;
import com.legend.parser.TypeExpression.FunctionType;
import com.legend.parser.TypeExpression.Generic;
import com.legend.parser.TypeExpression.NameRef;
import com.legend.parser.TypeExpression.Op;
import com.legend.parser.TypeExpression.RelationType;
import com.legend.parser.TypeExpression.SchemaAlgebra;
import com.legend.parser.TypeExpression.TypedParameter;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.legend.parser.TypeExpressionFixtures.col;
import static com.legend.parser.TypeExpressionFixtures.nr;
import static com.legend.parser.TypeExpressionFixtures.rel;
import static com.legend.parser.TypeExpressionFixtures.sa;
import static com.legend.parser.TypeExpressionFixtures.tg;
import static com.legend.parser.TypeExpressionFixtures.tp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void catalogMatchesTheGoldenFile() throws Exception {
        // THE golden catalog: every signature, canonically rendered, in
        // (load-bearing) declaration order. Replaces the count-pin + comment
        // changelog: any add/remove/edit/REORDER is a reviewable line diff.
        // To update deliberately: fix the code, regenerate the resource with
        // the renderer below, and review the diff in the commit.
        List<String> expected = java.nio.file.Files.readAllLines(
                        java.nio.file.Path.of("src/test/resources/native-catalog.txt"))
                .stream().filter(l -> !l.startsWith("#")).toList();
        List<String> actual = Pure.all().stream()
                .map(NativeFunctionTest::renderCanonical).toList();
        assertEquals(expected, actual,
                "the native catalog diverged from the golden file — review the diff;"
                        + " regenerate the resource only for DELIBERATE catalog changes");
    }

    /** Canonical signature rendering — the golden file's line format. */
    static String renderCanonical(NativeFunctionDefinition d) {
        StringBuilder s = new StringBuilder(d.qualifiedName());
        if (!d.typeParameters().isEmpty() || !d.multiplicityParameters().isEmpty()) {
            s.append('<').append(String.join(",", d.typeParameters()));
            if (!d.multiplicityParameters().isEmpty()) {
                s.append('|').append(String.join(",", d.multiplicityParameters()));
            }
            s.append('>');
        }
        s.append('(');
        for (int i = 0; i < d.parameters().size(); i++) {
            var p = d.parameters().get(i);
            if (i > 0) {
                s.append(", ");
            }
            s.append(p.name()).append(':').append(renderType(p.type())).append(p.multiplicity());
        }
        return s.append("):").append(renderType(d.returnType()))
                .append(d.returnMultiplicity()).toString();
    }

    private static String renderType(com.legend.parser.TypeExpression t) {
        return switch (t) {
            case com.legend.parser.TypeExpression.NameRef n -> n.name();
            case com.legend.parser.TypeExpression.Generic g -> g.name() + "<"
                    + String.join(",", g.arguments().stream()
                            .map(NativeFunctionTest::renderType).toList()) + ">";
            case com.legend.parser.TypeExpression.FunctionType f -> "{"
                    + String.join(",", f.parameters().stream()
                            .map(pp -> renderType(pp.type()) + pp.multiplicity()).toList())
                    + "->" + renderType(f.result().type()) + f.result().multiplicity() + "}";
            case com.legend.parser.TypeExpression.RelationType r -> "("
                    + String.join(",", r.columns().stream()
                            .map(c -> c.name() + ":" + renderType(c.type())).toList()) + ")";
            case com.legend.parser.TypeExpression.SchemaAlgebra a ->
                    renderType(a.left()) + a.op() + renderType(a.right());
        };
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
        TypeExpression relationOfT = tg(Pure.RELATION, nr("T"));
        TypeExpression filterFn = tg(Pure.FUNCTION, new FunctionType(
                List.of(tp(nr("T"), Multiplicity.exactly(1))),
                tp(nr(Pure.BOOLEAN), Multiplicity.exactly(1))));
        var expected = new NativeFunctionDefinition(
                "meta::pure::functions::relation::filter",
                List.of("T"),
                List.of(),
                List.of(
                        new ParameterDefinition("rel", relationOfT, Multiplicity.exactly(1)),
                        new ParameterDefinition("f", filterFn, Multiplicity.exactly(1))),
                relationOfT,
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
                "meta::pure::functions::lang::cast",
                List.of("T"),
                List.of("m"),
                List.of(
                        new ParameterDefinition("source", nr(Pure.ANY),
                                new Multiplicity.Parameter("m")),
                        new ParameterDefinition("type", nr("T"), Multiplicity.exactly(1))),
                nr("T"),
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
        // Wildcard column (?:K) parses as RelationType([Column("?",NameRef("K"),[1])]).
        TypeExpression wildcardOfK = rel(col("?", nr("K"), Multiplicity.exactly(1)));
        // Z=(?:K)⊆T parses left-associatively: SUBSET wraps the EQUAL
        // node, matching engine's parseTypeWithOperation precedence.
        TypeExpression zEqQK_subT = sa(
                sa(nr("Z"), Op.EQUAL, wildcardOfK),
                Op.SUBSET, nr("T"));
        // V=(?:K) has no SUBSET tail — just the EQUAL.
        TypeExpression vEqQK = sa(nr("V"), Op.EQUAL, wildcardOfK);
        // T-Z+V is left-leaning: (T-Z)+V.
        TypeExpression tMinusZPlusV = sa(
                sa(nr("T"), Op.DIFFERENCE, nr("Z")),
                Op.UNION, nr("V"));
        var expected = new NativeFunctionDefinition(
                "meta::pure::functions::relation::rename",
                List.of("T", "Z", "K", "V"),
                List.of(),
                List.of(
                        new ParameterDefinition("r",
                                tg(Pure.RELATION, nr("T")),
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("old",
                                tg(Pure.COL_SPEC, zEqQK_subT),
                                Multiplicity.exactly(1)),
                        new ParameterDefinition("new",
                                tg(Pure.COL_SPEC, vEqQK),
                                Multiplicity.exactly(1))),
                tg(Pure.RELATION, tMinusZPlusV),
                Multiplicity.exactly(1),
                List.of(),
                List.of());
        assertEquals(expected, Pure.RENAME__RELATION_1__COL_SPEC_1__COL_SPEC_1);
    }

    @Test
    void ifWithEmptyArgFunctionType_pinShape() {
        // Real legend-pure: if<T|m>(Boolean[1], Function<{->T[m]}>[1], Function<{->T[m]}>[1]): T[m]
        // Exercises the empty-parameter-list function-type grammar `{->T[m]}` with a multiplicity var.
        TypeExpression thunkOfT = tg(Pure.FUNCTION, new FunctionType(
                List.of(),
                tp(nr("T"), Multiplicity.parameter("m"))));
        var expected = new NativeFunctionDefinition(
                "meta::pure::functions::lang::if",
                List.of("T"),
                List.of("m"),
                List.of(
                        new ParameterDefinition("test", nr(Pure.BOOLEAN), Multiplicity.exactly(1)),
                        new ParameterDefinition("then", thunkOfT, Multiplicity.exactly(1)),
                        new ParameterDefinition("else", thunkOfT, Multiplicity.exactly(1))),
                nr("T"),
                Multiplicity.parameter("m"),
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
        assertEquals("meta::pure::functions::relation::extend", def.qualifiedName());
        assertEquals(List.of("T", "Z", "W", "R"), def.typeParameters());
        assertEquals(List.of(), def.multiplicityParameters());
        assertEquals(3, def.parameters().size());
        assertEquals(tg(Pure.WINDOW, nr("T")), def.parameters().get(1).type());
        // Structural pin of the nested function type with three input arrows.
        TypeExpression innerFn = new FunctionType(
                List.of(
                        tp(tg(Pure.RELATION, nr("T")), Multiplicity.exactly(1)),
                        tp(tg(Pure.WINDOW, nr("T")), Multiplicity.exactly(1)),
                        tp(nr("T"), Multiplicity.exactly(1))),
                tp(nr(Pure.ANY), Multiplicity.range(0, 1)));
        assertEquals(tg(Pure.FUNC_COL_SPEC, innerFn, nr("R")),
                def.parameters().get(2).type());
        // Return type Relation<T+R> = Generic(Relation, [SchemaAlgebra(T, UNION, R)]).
        assertEquals(tg(Pure.RELATION, sa(nr("T"), Op.UNION, nr("R"))),
                def.returnType());
        assertEquals(Multiplicity.exactly(1), def.returnMultiplicity());
    }

    @Test
    void sortWithSubsetConstraintMultiplicityMany_pinShape() {
        // sort<X,T>(Relation<T>[1], SortInfo<X \u2286 T>[*]): Relation<T>[1]
        TypeExpression relationOfT = tg(Pure.RELATION, nr("T"));
        TypeExpression sortInfoXsubT = tg(Pure.SORT_INFO,
                sa(nr("X"), Op.SUBSET, nr("T")));
        var expected = new NativeFunctionDefinition(
                "meta::pure::functions::relation::sort",
                List.of("X", "T"),
                List.of(),
                List.of(
                        new ParameterDefinition("rel", relationOfT, Multiplicity.exactly(1)),
                        new ParameterDefinition("sortInfo", sortInfoXsubT, Multiplicity.zeroMany())),
                relationOfT,
                Multiplicity.exactly(1),
                List.of(),
                List.of());
        assertEquals(expected, Pure.SORT__RELATION_1__SORT_INFO_MANY);
    }

    @Test
    void zeroArityNative_pinShape() {
        // generateGuid(): String[1]
        var expected = new NativeFunctionDefinition(
                "meta::pure::functions::string::generation::generateGuid",
                List.of(),
                List.of(),
                List.of(),
                nr(Pure.STRING),
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
                "graphFetch", "serialize", "generateGuid")) {
            assertTrue(simpleNames.contains(required),
                    () -> "required native '" + required
                            + "' missing from Pure catalog");
        }
    }

    // ===============================================================
    // Native class catalog ({@link Pure#allNativeClasses}).
    //
    // Same shape contract as the function catalog: class-load fails
    // loudly if any declaration stops parsing, and the tests below pin
    // structural invariants (hierarchy edges, type-parameter arity,
    // headline presence, the isNative flag).
    // ===============================================================

    @Test
    void nativeClassCatalogSizeIsPinned() {
        // Update this deliberately when adding or removing native classes.
        // 37: +StrictTime (real legend-pure meta::pure::metamodel::type::StrictTime).
        assertEquals(38, Pure.allNativeClasses().size(),
                "Pure.allNativeClasses() size pin: review the catalog if this changes");
    }

    @Test
    void everyNativeClassIsMarkedNativeAndHasEmptyBody() {
        for (ClassDefinition c : Pure.allNativeClasses()) {
            assertTrue(c.isNative(),
                    () -> "native class '" + c.qualifiedName() + "' has isNative=false");
            assertTrue(c.properties().isEmpty(),
                    () -> "native class '" + c.qualifiedName()
                            + "' must have empty properties for now (got "
                            + c.properties() + ")");
            assertTrue(c.derivedProperties().isEmpty(),
                    () -> "native class '" + c.qualifiedName()
                            + "' must have empty derived properties");
            assertTrue(c.constraints().isEmpty(),
                    () -> "native class '" + c.qualifiedName()
                            + "' must have empty constraints");
        }
    }

    @Test
    void everyNativeClassHasUniqueFqn() {
        Set<String> seen = new HashSet<>();
        for (ClassDefinition c : Pure.allNativeClasses()) {
            assertTrue(seen.add(c.qualifiedName()),
                    () -> "duplicate native class FQN: " + c.qualifiedName());
        }
    }

    @Test
    void numericTowerHierarchyIsCorrect() {
        // Integer/Float/Decimal extend Number; Number extends Any.
        assertEquals(List.of(nr(Pure.ANY)), Pure.NUMBER.superClasses());
        assertEquals(List.of(nr(Pure.NUMBER)), Pure.INTEGER.superClasses());
        assertEquals(List.of(nr(Pure.NUMBER)), Pure.FLOAT.superClasses());
        assertEquals(List.of(nr(Pure.NUMBER)), Pure.DECIMAL.superClasses());
    }

    @Test
    void dateHierarchyIsCorrect() {
        // Date extends Any; StrictDate/DateTime/LatestDate extend Date.
        assertEquals(List.of(nr(Pure.ANY)),  Pure.DATE.superClasses());
        assertEquals(List.of(nr(Pure.DATE)), Pure.STRICT_DATE.superClasses());
        assertEquals(List.of(nr(Pure.DATE)), Pure.DATE_TIME.superClasses());
        assertEquals(List.of(nr(Pure.DATE)), Pure.LATEST_DATE.superClasses());
    }

    @Test
    void anyHasNoSuperclass() {
        // Top of the hierarchy: Any must have no supers.
        assertTrue(Pure.ANY.superClasses().isEmpty(),
                () -> "Any must have no superclasses, got " + Pure.ANY.superClasses());
    }

    @Test
    void parameterizedNativeClassesCarryTypeParameters() {
        // Single-parameter generics.
        assertEquals(List.of("T"), Pure.RELATION.typeParams());
        assertEquals(List.of("T"), Pure.COL_SPEC.typeParams());
        assertEquals(List.of("T"), Pure.COL_SPEC_ARRAY.typeParams());
        assertEquals(List.of("T"), Pure.WINDOW.typeParams());
        assertEquals(List.of("T"), Pure.SORT_INFO.typeParams());
        assertEquals(List.of("F"), Pure.FUNCTION.typeParams());
        // Two-parameter generics.
        assertEquals(List.of("F", "R"), Pure.FUNC_COL_SPEC.typeParams());
        assertEquals(List.of("F", "R"), Pure.FUNC_COL_SPEC_ARRAY.typeParams());
        // Three-parameter generics.
        assertEquals(List.of("F", "U", "R"), Pure.AGG_COL_SPEC.typeParams());
        assertEquals(List.of("F", "U", "R"), Pure.AGG_COL_SPEC_ARRAY.typeParams());
    }

    @Test
    void nonParameterizedNativeClassesHaveNoTypeParameters() {
        // The primitives, Any/Type/Nil, and _Traversal carry no type
        // parameters. Pinning this catches accidental drift where a
        // declaration grows an unintended <T>.
        for (ClassDefinition c : List.of(
                Pure.ANY, Pure.TYPE, Pure.NIL,
                Pure.NUMBER, Pure.INTEGER, Pure.FLOAT, Pure.DECIMAL,
                Pure.STRING, Pure.BOOLEAN, Pure.BYTE,
                Pure.DATE, Pure.STRICT_DATE, Pure.DATE_TIME, Pure.LATEST_DATE,
                Pure.TRAVERSAL)) {
            assertTrue(c.typeParams().isEmpty(),
                    () -> "expected no type params on " + c.qualifiedName()
                            + ", got " + c.typeParams());
        }
    }

    @Test
    void headlineNativeClassesAreAllPresent() {
        Set<String> simpleNames = Pure.allNativeClasses().stream()
                .map(c -> simpleName(c.qualifiedName()))
                .collect(Collectors.toSet());
        for (String required : List.of(
                "Any", "Nil", "Type",
                "Number", "Integer", "Float", "Decimal",
                "String", "Boolean", "Byte",
                "Date", "StrictDate", "DateTime", "LatestDate",
                "Relation", "ColSpec", "FuncColSpec", "AggColSpec",
                "Function",
                "_Window", "_Traversal", "SortInfo")) {
            assertTrue(simpleNames.contains(required),
                    () -> "required native class '" + required
                            + "' missing from Pure.allNativeClasses()");
        }
    }

    @Test
    void nativeClassFqnsAreInExpectedPackages() {
        // Every native FQN must live under one of the documented packages;
        // a typo or stray declaration that leaks elsewhere is a bug.
        List<String> expected = List.of(
                Pure.TYPE_PKG, Pure.RELATION_PKG, Pure.FUNCTION_PKG,
                Pure.RELATION_FUNCTIONS_PKG, Pure.COLLECTION_PKG,
                Pure.MATH_UTILITY_PKG, Pure.VARIANT_PKG, Pure.GRAPH_FETCH_PKG,
                // ModelElement lives directly under meta::pure::metamodel
                // (real M3's package tree root element).
                "meta::pure::metamodel");
        for (ClassDefinition c : Pure.allNativeClasses()) {
            String fqn = c.qualifiedName();
            boolean ok = expected.stream().anyMatch(p -> fqn.startsWith(p + "::"));
            assertTrue(ok, () -> "native class FQN outside expected packages: " + fqn);
        }
    }

    // ===============================================================
    // Native enum catalog ({@link Pure#allNativeEnums}).
    //
    // Engine declares several stdlib types as {@code Enum} rather than
    // {@code Class} (DurationUnit, JoinKind, ...). They round-trip
    // through {@link ElementParser} the same way native classes do.
    // ===============================================================

    @Test
    void nativeEnumCatalogSizeIsPinned() {
        // Update this deliberately when adding or removing native enums.
        assertEquals(8, Pure.allNativeEnums().size(),
                "Pure.allNativeEnums() size pin: review the catalog if this changes");
    }

    @Test
    void everyNativeEnumHasAtLeastOneValueAndUniqueValues() {
        for (EnumDefinition e : Pure.allNativeEnums()) {
            assertFalse(e.values().isEmpty(),
                    () -> "native enum '" + e.qualifiedName() + "' has no values");
            Set<String> seen = new HashSet<>(e.values());
            assertEquals(e.values().size(), seen.size(),
                    () -> "native enum '" + e.qualifiedName()
                            + "' has duplicate values: " + e.values());
        }
    }

    @Test
    void everyNativeEnumHasUniqueFqn() {
        Set<String> seen = new HashSet<>();
        for (EnumDefinition e : Pure.allNativeEnums()) {
            assertTrue(seen.add(e.qualifiedName()),
                    () -> "duplicate native enum FQN: " + e.qualifiedName());
        }
    }

    @Test
    void headlineNativeEnumValuesArePinned() {
        // Spot-check the enums most consumers reach for. If engine extends
        // any of these we'll catch it here before downstream code breaks.
        assertEquals(List.of("ASC", "DESC"), Pure.SORT_TYPE.values());
        assertEquals(List.of("LEFT", "RIGHT", "FULL", "INNER"), Pure.JOIN_KIND.values());
        assertEquals(List.of("MD5", "SHA1", "SHA256"), Pure.HASH_TYPE.values());
        assertEquals(List.of("Q1", "Q2", "Q3", "Q4"), Pure.QUARTER.values());
        assertEquals(10, Pure.DURATION_UNIT.values().size(),
                "DurationUnit has YEARS..NANOSECONDS = 10 values");
        assertEquals(12, Pure.MONTH.values().size(),
                "Month has January..December = 12 values");
        assertEquals(7, Pure.DAY_OF_WEEK.values().size(),
                "DayOfWeek has Monday..Sunday = 7 values");
    }

    // ===============================================================
    // Coverage: every FQN referenced in a native function signature
    // (parameter types, return type, generic arguments) MUST resolve
    // to a record in the class or enum catalog. This is the single
    // tenet that keeps natives and types consistent: nothing in the
    // signatures dangles unresolved.
    //
    // If this fails: either declare the missing type in {@link Pure}
    // or remove the offending native.
    // ===============================================================

    @Test
    void everyTypePositionFqnInNativeSignaturesResolvesToCatalog() {
        Set<String> catalogFqns = new HashSet<>();
        Pure.allNativeClasses().forEach(c -> catalogFqns.add(c.qualifiedName()));
        Pure.allNativeEnums().forEach(e -> catalogFqns.add(e.qualifiedName()));

        java.util.SortedSet<String> missing = new java.util.TreeSet<>();
        for (NativeFunctionDefinition def : Pure.all()) {
            Set<String> typeParams = Set.copyOf(def.typeParameters());
            for (var p : def.parameters()) {
                collectTypePositionFqns(p.type(), typeParams, catalogFqns, missing);
            }
            collectTypePositionFqns(def.returnType(), typeParams, catalogFqns, missing);
        }
        assertTrue(missing.isEmpty(),
                () -> "FQNs referenced in native signatures but missing from "
                        + "Pure.allNativeClasses() / Pure.allNativeEnums():\n  "
                        + String.join("\n  ", missing));
    }

    /**
     * Walks a {@link TypeExpression} and records every FQN (anything with
     * {@code ::}) that appears in a type position. Bare names (no {@code ::})
     * are skipped &mdash; they're type-parameter binders. Recurses into
     * generic arguments, function-type parameter/return types, relation-type
     * columns, and schema-algebra operands.
     */
    private static void collectTypePositionFqns(
            TypeExpression t,
            Set<String> typeParams,
            Set<String> catalog,
            Set<String> missing) {
        switch (t) {
            case NameRef nr -> recordIfFqn(nr.name(), typeParams, catalog, missing);
            case Generic g -> {
                recordIfFqn(g.name(), typeParams, catalog, missing);
                for (TypeExpression arg : g.arguments()) {
                    collectTypePositionFqns(arg, typeParams, catalog, missing);
                }
            }
            case FunctionType ft -> {
                for (TypedParameter p : ft.parameters()) {
                    collectTypePositionFqns(p.type(), typeParams, catalog, missing);
                }
                collectTypePositionFqns(ft.result().type(), typeParams, catalog, missing);
            }
            case RelationType rt -> {
                for (Column c : rt.columns()) {
                    collectTypePositionFqns(c.type(), typeParams, catalog, missing);
                }
            }
            case SchemaAlgebra sa -> {
                collectTypePositionFqns(sa.left(), typeParams, catalog, missing);
                collectTypePositionFqns(sa.right(), typeParams, catalog, missing);
            }
            default -> {
                // Other variants (if any) carry no FQN-shaped references.
            }
        }
    }

    private static void recordIfFqn(
            String name,
            Set<String> typeParams,
            Set<String> catalog,
            Set<String> missing) {
        // FQNs always contain '::'. Bare names like 'T', 'K', 'm' are type
        // parameters; they have no FQN to resolve.
        if (!name.contains("::")) {
            return;
        }
        if (!catalog.contains(name)) {
            missing.add(name);
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
