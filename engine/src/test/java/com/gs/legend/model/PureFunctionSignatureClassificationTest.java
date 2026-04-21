package com.gs.legend.model;

import com.gs.legend.model.m3.PureFunction;
import com.gs.legend.model.m3.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Targeted coverage for {@link PureModelBuilder#buildPureFunction}'s signature
 * classification pass (see {@code classifyFunctionSigType}). Each test exercises a
 * distinct branch of the {@code Type} switch — flat leaves, nested structural carriers,
 * and the lenient fallback for unresolved names — so the whole walk is covered.
 *
 * <p>Anti-goal: do not duplicate what the parser tests already pin (e.g. that
 * {@code Integer[1]} parses to {@link com.gs.legend.model.m3.Primitive#INTEGER}). Those
 * paths do not touch {@code classifyFunctionSigType}.
 */
class PureFunctionSignatureClassificationTest {

    @Test
    void flatClassAndEnumParamsAreClassifiedAndNothingLeaksAsNameRef() {
        // One function with mixed kinds exercises the flat NameRef branch for both
        // ClassType and EnumType, and proves the no-leak invariant end-to-end.
        String source = """
                Class model::Firm { name: String[1]; }
                Enum model::Country { US, UK }
                function model::report(f: model::Firm[1], c: model::Country[1]): model::Firm[1]
                {
                  $f
                }
                """;

        PureFunction pf = new PureModelBuilder().addSource(source)
                .findFunction("model::report").getFirst();

        assertEquals(new Type.ClassType("model::Firm"), pf.parameters().get(0).type(),
                "User class parameter must classify to ClassType");
        assertEquals(new Type.EnumType("model::Country"), pf.parameters().get(1).type(),
                "User enum parameter must classify to EnumType");
        assertEquals(new Type.ClassType("model::Firm"), pf.returnType(),
                "User class return must classify to ClassType");

        for (var p : pf.parameters()) {
            assertFalse(p.type() instanceof Type.NameRef,
                    "Parameter '" + p.name() + "' leaked as NameRef: " + p.type());
        }
        assertFalse(pf.returnType() instanceof Type.NameRef,
                "Return leaked as NameRef: " + pf.returnType());
    }

    @Test
    void userClassNestedInsideRelationSchemaIsClassified() {
        // Relation<(owner: model::Firm[1], ...)> exercises the RelationTypeVar +
        // GenericType branches of the walk. The column NameRef must not be left
        // unresolved.
        String source = """
                Class model::Firm { name: String[1]; }
                function model::firmsWithOwners(
                    data: Relation<(name: String, owner: model::Firm)>[1]
                ): Relation<(name: String, owner: model::Firm)>[1]
                {
                  $data
                }
                """;

        PureFunction pf = new PureModelBuilder().addSource(source)
                .findFunction("model::firmsWithOwners").getFirst();

        Type.Schema paramSchema = extractRelationSchema(pf.parameters().getFirst().type());
        assertNotNull(paramSchema, "Parameter did not carry a Relation schema");
        assertEquals(new Type.ClassType("model::Firm"), paramSchema.columns().get("owner"),
                "User class column inside Relation<(...)> must classify to ClassType");

        Type.Schema returnSchema = extractRelationSchema(pf.returnType());
        assertNotNull(returnSchema, "Return did not carry a Relation schema");
        assertEquals(new Type.ClassType("model::Firm"), returnSchema.columns().get("owner"),
                "User class column in the return schema must classify too");
    }

    @Test
    void userClassNestedInsideFunctionTypeCallbackIsClassified() {
        // A callback-typed parameter exercises the FunctionType recursion branch —
        // the inner param's NameRef must classify.
        // Syntax is Pure's bracketed functional-literal form.
        String source = """
                Class model::Firm { name: String[1]; }
                function model::applyToFirm(
                    f: Function<{model::Firm[1]->String[1]}>[1]
                ): String[1]
                {
                  ''
                }
                """;

        PureFunction pf = new PureModelBuilder().addSource(source)
                .findFunction("model::applyToFirm").getFirst();

        Type paramType = pf.parameters().getFirst().type();
        Type.FunctionType ft = findFunctionType(paramType);
        assertNotNull(ft, "Parameter type did not carry a FunctionType: " + paramType);
        assertEquals(1, ft.params().size(), "Callback must have one param");
        assertEquals(new Type.ClassType("model::Firm"), ft.params().getFirst().type(),
                "User class inside FunctionType callback must classify to ClassType");
    }

    @Test
    void userClassNestedDeepInsideFunctionReturningFromRelationIsClassified() {
        // Deep nesting: Function<{Relation<(col: model::Firm)>[1] -> String[1]}>.
        // Classification must reach through FunctionType -> its parameter's GenericType ->
        // RelationTypeVar -> column NameRef. Any missing recursion leaves the inner
        // NameRef unresolved.
        String source = """
                Class model::Firm { name: String[1]; }
                function model::applyToRel(
                    f: Function<{Relation<(name: String, owner: model::Firm)>[1] -> String[1]}>[1]
                ): String[1]
                {
                  ''
                }
                """;

        PureFunction pf = new PureModelBuilder().addSource(source)
                .findFunction("model::applyToRel").getFirst();

        Type.FunctionType callback = findFunctionType(pf.parameters().getFirst().type());
        assertNotNull(callback, "Parameter did not carry a FunctionType");
        Type.Schema innerSchema = extractRelationSchema(callback.params().getFirst().type());
        assertNotNull(innerSchema, "Callback's first param did not carry a Relation schema");
        assertEquals(new Type.ClassType("model::Firm"), innerSchema.columns().get("owner"),
                "Classification must reach a NameRef four levels deep "
                        + "(FunctionType -> param -> Relation -> column)");
    }

    @Test
    void mixedResolvedAndUnresolvedTypesInSameSigAreClassifiedPerLeaf() {
        // Per-leaf independence: known FQNs classify, unknown ones stay as NameRef,
        // in the same signature. Guards against an accidental all-or-nothing guard.
        String source = """
                Class model::Known { id: Integer[1]; }
                function model::mixed(k: model::Known[1], u: other::Unknown[1]): Integer[1]
                {
                  1
                }
                """;

        PureFunction pf = new PureModelBuilder().addSource(source)
                .findFunction("model::mixed").getFirst();

        assertEquals(new Type.ClassType("model::Known"), pf.parameters().get(0).type(),
                "Known user class must classify even when a sibling param is unresolved");
        assertInstanceOf(Type.NameRef.class, pf.parameters().get(1).type(),
                "Unknown user class must stay as NameRef even when a sibling param is classified");
    }

    @Test
    void classificationIsIdentityNoOpWhenNothingChanges() {
        // All-primitive / all-classified signatures must short-circuit without
        // allocating new wrappers. A regression here doubles allocations per
        // function, which shows up at 100k+ scale. We check reference identity.
        String source = """
                function model::addInts(x: Integer[1], y: Integer[1]): Integer[1]
                {
                  $x + $y
                }
                """;

        var builder = new PureModelBuilder().addSource(source);
        PureFunction pf = builder.findFunction("model::addInts").getFirst();

        Type rawReturn = builder.findFunctionDefinition("model::addInts")
                .getFirst().parsedReturnType();
        assertSame(rawReturn, pf.returnType(),
                "Primitive-only sig should not reallocate — classification is a no-op");
    }

    @Test
    void unknownUserTypeStaysAsNameRefForLateBinding() {
        // The whole point of the lenient contract: a function may reference a user
        // class not yet registered. The NameRef must survive so the TypeChecker can
        // resolve it later when the class source is added.
        String source = """
                function model::usesMissing(x: other::Missing[1]): Integer[1]
                {
                  1
                }
                """;

        PureFunction pf = new PureModelBuilder().addSource(source)
                .findFunction("model::usesMissing").getFirst();

        assertInstanceOf(Type.NameRef.class, pf.parameters().getFirst().type(),
                "Unresolved user class must pass through as NameRef under the lenient contract");
    }

    @Test
    void crossSourceLateBindingRetroactivelyClassifiesEarlierFunctionSigs() {
        // Lenient contract's payoff: when a function is ingested before the class it
        // references, its signature starts as NameRef. A subsequent addSource that
        // declares the class triggers buildPureFunctions to rerun over all ingested
        // functions, and the earlier signature is retroactively promoted to ClassType.
        //
        // This is convenient at small scale. Note the cost: rebuild is O(N) per
        // addSource, so M sources over N functions is O(N x M) — the motivation for
        // a future addSources batch entry that can classify once over a frozen set.
        String funcSource = """
                function model::usesFirm(f: other::Firm[1]): String[1]
                {
                  $f.name
                }
                """;
        String classSource = """
                Class other::Firm { name: String[1]; }
                """;

        var builder = new PureModelBuilder().addSource(funcSource);

        // Pre: class source not yet added, so signature carries NameRef.
        PureFunction pfBefore = builder.findFunction("model::usesFirm").getFirst();
        assertInstanceOf(Type.NameRef.class, pfBefore.parameters().getFirst().type(),
                "Before class source is added, the unresolved FQN must carry as NameRef");

        builder.addSource(classSource);

        // Post: rerun classified the signature. findFunction returns the rebuilt
        // PureFunction with the promoted type.
        PureFunction pfAfter = builder.findFunction("model::usesFirm").getFirst();
        assertEquals(new Type.ClassType("other::Firm"),
                pfAfter.parameters().getFirst().type(),
                "After the class source is added, the signature must be retroactively classified");
    }

    // ---- helpers ----

    /** Extracts a Relation schema from a Type, whether classified or still parse-layer. */
    private static Type.Schema extractRelationSchema(Type t) {
        if (t instanceof Type.Relation rel) return rel.schema();
        if (t instanceof Type.GenericType gt
                && !gt.typeArgs().isEmpty()
                && gt.typeArgs().getFirst() instanceof Type.RelationTypeVar rtv) {
            var cols = new java.util.LinkedHashMap<String, Type>();
            for (var c : rtv.columns()) cols.put(c.name(), c.type());
            return Type.Schema.withoutPivot(cols);
        }
        return null;
    }

    /** Extracts the inner FunctionType from a classified user-function callback param. */
    private static Type.FunctionType findFunctionType(Type t) {
        if (t instanceof Type.FunctionType ft) return ft;
        if (t instanceof Type.GenericType gt) {
            for (Type arg : gt.typeArgs()) {
                Type.FunctionType inner = findFunctionType(arg);
                if (inner != null) return inner;
            }
        }
        return null;
    }
}
