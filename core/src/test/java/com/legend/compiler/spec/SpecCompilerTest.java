package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.builtin.Pure;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.parser.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.parser.ParsedModel;
import com.legend.parser.SpecParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the first Phase-G body-checker slice ({@link SpecCompiler}): scalar
 * literals, variable references, and the bidirectional {@code Check} conformance
 * (PHASE_G_SPEC_COMPILER.md §2/§6, G.1).
 */
class SpecCompilerTest {

    private static SpecCompiler compilerWith(String model) {
        ParsedModel p = ElementParser.parse(model);
        return new SpecCompiler(PureModelContext.from(
                new NormalizedModel(p.elements(), p.imports())));
    }

    private static SpecCompiler compiler() {
        return compilerWith("Class model::Person {}\n");
    }

    private static final String STR = Type.Primitive.STRING.qualifiedName();
    private static final String INT = Type.Primitive.INTEGER.qualifiedName();
    private static final String BOOL = Type.Primitive.BOOLEAN.qualifiedName();

    /** Parse a model, find the one overload of {@code fnFqn}, and type-check its body. */
    private static CompiledFunction checkOnly(String model, String fnFqn) {
        ParsedModel p = ElementParser.parse(model);
        PureModelContext c = PureModelContext.from(new NormalizedModel(p.elements(), p.imports()));
        List<TypedFunction> fns = c.findFunction(fnFqn);
        assertEquals(1, fns.size(), fnFqn + " should have exactly one overload");
        return new SpecCompiler(c).compile(fns.get(0));
    }

    /** A Person/Address graph in post-NameResolver (FQN) form, for property-access tests. */
    private static final String PERSON_MODEL =
            "Class model::Address { street: " + STR + "[1]; }\n"
          + "Class model::Person {\n"
          + "  name: " + STR + "[1];\n"
          + "  age: " + INT + "[1];\n"
          + "  home: model::Address[0..1];\n"
          + "  friends: model::Person[*];\n"
          + "}\n";

    private static ExprType one(Type t) {
        return new ExprType(t, Multiplicity.Bounded.ONE);
    }

    /** Type-check {@code body} with {@code $this : model::Person[1]} in scope. */
    private static TypedSpec onPerson(String body) {
        Env env = Env.empty().with("this",
                new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ONE));
        return compilerWith(PERSON_MODEL).typeBody(SpecParser.parse(body), env, Expected.infer());
    }

    private static TypedSpec infer(String src) {
        return compiler().typeBody(SpecParser.parse(src), Env.empty(), Expected.infer());
    }

    // ---- literals -------------------------------------------------------

    @Test
    void integerLiteral() {
        TypedSpec n = infer("5");
        assertInstanceOf(TypedCInteger.class, n);
        assertEquals(one(Type.Primitive.INTEGER), n.info());
    }

    @Test
    void stringLiteral() {
        TypedSpec n = infer("'hi'");
        assertInstanceOf(TypedCString.class, n);
        assertEquals(one(Type.Primitive.STRING), n.info());
    }

    @Test
    void booleanLiteral() {
        assertEquals(one(Type.Primitive.BOOLEAN), infer("true").info());
    }

    @Test
    void floatLiteral() {
        assertEquals(one(Type.Primitive.FLOAT), infer("1.5").info());
    }

    @Test
    void decimalLiteral_carriesScaleFromText() {
        TypedSpec n = infer("1.50d");
        assertInstanceOf(TypedCDecimal.class, n);
        assertEquals(one(new Type.PrecisionDecimal(38, 2)), n.info());
    }

    // ---- variables ------------------------------------------------------

    @Test
    void variable_resolvesTypeFromEnvironment() {
        Env env = Env.empty().with("x", one(Type.Primitive.INTEGER));
        TypedSpec n = compiler().typeBody(SpecParser.parse("$x"), env, Expected.infer());
        assertInstanceOf(TypedVariable.class, n);
        assertEquals("x", ((TypedVariable) n).name());
        assertEquals(one(Type.Primitive.INTEGER), n.info());
    }

    @Test
    void variable_unboundThrows() {
        assertThrows(TypeInferenceException.class, () -> compiler().typeBody(
                SpecParser.parse("$y"), Env.empty(), Expected.infer()));
    }

    // ---- bidirectional Check mode --------------------------------------

    @Test
    void check_conformsToSupertype() {
        // 5 : Integer[1] checked against Number[1] -> conforms (no throw).
        compiler().typeBody(SpecParser.parse("5"), Env.empty(),
                Expected.check(one(Type.Primitive.NUMBER)));
    }

    @Test
    void check_rejectsNonConformingType() {
        // 5 : Integer[1] checked against String[1] -> throws.
        assertThrows(TypeInferenceException.class, () -> compiler().typeBody(
                SpecParser.parse("5"), Env.empty(), Expected.check(one(Type.Primitive.STRING))));
    }

    @Test
    void infer_unimplementedFormThrowsClearly() {
        // A bare (non-thunk) lambda is a later slice — must fail loudly, not silently mis-handle.
        assertThrows(TypeInferenceException.class, () -> infer("x | $x"));
    }

    // ---- native calls ---------------------------------------------------

    @Test
    void nativeCall_resolvesResultFromSignature() {
        // length(String[1]):Integer[1]
        TypedSpec n = infer("length('hello')");
        assertInstanceOf(TypedNativeCall.class, n);
        assertEquals("length", ((TypedNativeCall) n).function());
        assertEquals(one(Type.Primitive.INTEGER), n.info());
    }

    @Test
    void nativeCall_anyParamsAcceptMixedArgs() {
        // equal(Any[1], Any[1]):Boolean[1]
        assertEquals(one(Type.Primitive.BOOLEAN), infer("equal(1, 'x')").info());
    }

    @Test
    void nativeCall_picksOverloadViaSubtype() {
        // lessThanEqual(Number,Number):Boolean — Integer args (Integer <: Number)
        assertEquals(one(Type.Primitive.BOOLEAN), infer("lessThanEqual(1, 2)").info());
    }

    @Test
    void nativeCall_nestedArgumentIsTypedFirst() {
        // equal(length('a'), 1): inner length -> Integer[1]; equal(Any,Any) -> Boolean[1]
        TypedSpec n = infer("equal(length('a'), 1)");
        assertInstanceOf(TypedNativeCall.class, n);
        assertEquals(one(Type.Primitive.BOOLEAN), n.info());
        // the first argument node is itself a typed native call
        assertInstanceOf(TypedNativeCall.class, ((TypedNativeCall) n).args().get(0));
    }

    @Test
    void nativeCall_unknownFunctionThrows() {
        assertThrows(TypeInferenceException.class, () -> infer("noSuchNativeFunction(1)"));
    }

    @Test
    void nativeCall_wrongArgumentTypeThrows() {
        // length expects String[1]; an Integer argument matches no overload.
        assertThrows(TypeInferenceException.class, () -> infer("length(5)"));
    }

    // ---- user calls -----------------------------------------------------

    @Test
    void userCall_resolvesToTypedUserCallCarryingCallee() {
        String s = Type.Primitive.STRING.qualifiedName();
        SpecCompiler c = compilerWith(
                "Class model::Person {}\n"
              + "function my::pkg::greet(name:" + s + "[1]):" + s + "[1] { 'hi' }\n");
        TypedSpec n = c.typeBody(SpecParser.parse("my::pkg::greet('bob')"), Env.empty(), Expected.infer());
        assertInstanceOf(TypedUserCall.class, n);
        assertEquals("my::pkg::greet", ((TypedUserCall) n).callee().qualifiedName());
        assertEquals(one(Type.Primitive.STRING), n.info());
    }

    // ---- property access ------------------------------------------------

    @Test
    void property_simpleAccess() {
        TypedSpec n = onPerson("$this.name");
        assertInstanceOf(TypedPropertyAccess.class, n);
        assertEquals("name", ((TypedPropertyAccess) n).property());
        assertEquals(one(Type.Primitive.STRING), n.info());
    }

    @Test
    void property_multiHopThroughOptionalIsOptional() {
        // this[1] . home[0..1] . street[1]  ->  String[0..1]
        assertEquals(new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_ONE),
                onPerson("$this.home.street").info());
    }

    @Test
    void property_multiHopThroughManyIsMany() {
        // this[1] . friends[*] . name[1]  ->  String[*]
        assertEquals(new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_MANY),
                onPerson("$this.friends.name").info());
    }

    @Test
    void property_onNonClassTypeThrows() {
        Env env = Env.empty().with("n", one(Type.Primitive.INTEGER));
        assertThrows(TypeInferenceException.class, () -> compilerWith(PERSON_MODEL)
                .typeBody(SpecParser.parse("$n.anything"), env, Expected.infer()));
    }

    @Test
    void property_unknownPropertyThrows() {
        assertThrows(TypeInferenceException.class, () -> onPerson("$this.notAProperty"));
    }

    @Test
    void property_feedsIntoNativeCall() {
        // length($this.name): property -> String[1] -> length -> Integer[1]
        assertEquals(one(Type.Primitive.INTEGER), onPerson("length($this.name)").info());
    }

    @Test
    void property_inheritedFromSuperclassResolves() {
        String model = "Class model::Entity { id: " + STR + "[1]; }\n"
                     + "Class model::Account extends model::Entity { balance: " + INT + "[1]; }\n";
        Env env = Env.empty().with("this",
                new ExprType(new Type.ClassType("model::Account"), Multiplicity.Bounded.ONE));
        TypedSpec n = compilerWith(model).typeBody(SpecParser.parse("$this.id"), env, Expected.infer());
        assertEquals(one(Type.Primitive.STRING), n.info());   // id is declared on the superclass
    }

    // ---- strengthened: value preservation, Check multiplicity, decimal clamp ----

    @Test
    void literals_preserveTheirParsedValue() {
        assertEquals(5L, ((TypedCInteger) infer("5")).value());
        assertEquals("hi", ((TypedCString) infer("'hi'")).value());
        assertEquals(true, ((com.legend.compiler.spec.typed.TypedCBoolean) infer("true")).value());
    }

    @Test
    void check_rejectsMultiplicityMismatch() {
        // $this.friends.name : String[*] checked against String[1] -> the [*]->[1] reject fires.
        Env env = Env.empty().with("this",
                new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ONE));
        assertThrows(TypeInferenceException.class, () -> compilerWith(PERSON_MODEL).typeBody(
                SpecParser.parse("$this.friends.name"), env, Expected.check(one(Type.Primitive.STRING))));
    }

    @Test
    void decimalLiteral_integerFormHasZeroScale() {
        // "5d" -> Decimal(38, 0): exercises the scale==0 clamp branch.
        assertEquals(one(new Type.PrecisionDecimal(38, 0)), infer("5d").info());
    }

    // ---- whole-function check (the Phase-G entry point) ----------------

    @Test
    void check_bodyConformsToDeclaredReturn() {
        CompiledFunction cf = checkOnly(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { $x }", "model::f");
        assertEquals(one(Type.Primitive.INTEGER), cf.result().info());
    }

    @Test
    void check_bodyViolatingDeclaredReturnThrows() {
        // body $x : Integer[1] does not conform to declared return String[1].
        assertThrows(TypeInferenceException.class, () -> checkOnly(
                "function model::f(x:" + INT + "[1]):" + STR + "[1] { $x }", "model::f"));
    }

    @Test
    void check_propertyNavigationBody() {
        CompiledFunction cf = checkOnly(
                PERSON_MODEL + "function model::f(p:model::Person[1]):" + STR + "[1] { $p.name }",
                "model::f");
        assertEquals(one(Type.Primitive.STRING), cf.result().info());
    }

    @Test
    void check_callBodyAgainstBooleanReturn() {
        // a constraint-shaped body: lessThanEqual(Number,Number):Boolean[1]
        CompiledFunction cf = checkOnly(
                "function model::f(x:" + INT + "[1]):" + BOOL + "[1] { lessThanEqual($x, 5) }", "model::f");
        assertEquals(one(Type.Primitive.BOOLEAN), cf.result().info());
    }

    @Test
    void check_multiStatementChecksOnlyTheLast() {
        CompiledFunction cf = checkOnly(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { 1; $x }", "model::f");
        assertEquals(2, cf.body().size());
        assertEquals(one(Type.Primitive.INTEGER), cf.result().info());
    }

    @Test
    void check_nativeFunctionHasNoBodyThrows() {
        ParsedModel p = ElementParser.parse("Class model::Person {}\n");
        PureModelContext c = PureModelContext.from(new NormalizedModel(p.elements(), p.imports()));
        TypedFunction nativeLength = c.findFunction("length").get(0);
        assertThrows(TypeInferenceException.class, () -> new SpecCompiler(c).compile(nativeLength));
    }

    // ---- demand-driven memoization + reachability ----------------------

    private static SpecCompiler compilerAndCtx(String model, PureModelContext[] outCtx) {
        ParsedModel p = ElementParser.parse(model);
        PureModelContext c = PureModelContext.from(new NormalizedModel(p.elements(), p.imports()));
        outCtx[0] = c;
        return new SpecCompiler(c);
    }

    @Test
    void require_compilesEachBodyExactlyOnce() {
        PureModelContext[] c = new PureModelContext[1];
        SpecCompiler compiler = compilerAndCtx(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { $x }\n", c);
        TypedFunction f = c[0].findFunction("model::f").get(0);
        assertSame(compiler.compile(f), compiler.compile(f));   // memoized: same instance
    }

    @Test
    void compileReachable_followsTheCallChain() {
        // f -> g -> h ; length(...) is native (no body) and is NOT compiled.
        PureModelContext[] c = new PureModelContext[1];
        SpecCompiler compiler = compilerAndCtx(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { model::g($x) }\n"
              + "function model::g(x:" + INT + "[1]):" + INT + "[1] { model::h($x) }\n"
              + "function model::h(x:" + INT + "[1]):" + INT + "[1] { length('a') }\n", c);
        TypedFunction f = c[0].findFunction("model::f").get(0);

        Set<String> names = compiler.compileReachable(f).stream()
                .map(cf -> cf.signature().qualifiedName()).collect(Collectors.toSet());
        assertEquals(Set.of("model::f", "model::g", "model::h"), names);   // natives excluded
    }

    // ---- collections ----------------------------------------------------

    private static ExprType exact(Type t, int n) {
        return new ExprType(t, new Multiplicity.Bounded(n, n));
    }

    @Test
    void collection_homogeneousHasExactCount() {
        TypedSpec n = infer("[1, 2, 3]");
        assertInstanceOf(TypedCollection.class, n);
        assertEquals(exact(Type.Primitive.INTEGER, 3), n.info());
    }

    @Test
    void collection_mixedNumericWidensToNumber() {
        // [1, 1.5] : Integer + Float -> Number[2]
        assertEquals(exact(Type.Primitive.NUMBER, 2), infer("[1, 1.5]").info());
    }

    @Test
    void collection_unrelatedTypesWidenToAny() {
        // ['a', 1] : String + Integer -> Any[2]
        assertEquals(exact(new Type.ClassType(Pure.ANY.qualifiedName()), 2), infer("['a', 1]").info());
    }

    @Test
    void collection_emptyIsAnyZero() {
        assertEquals(exact(new Type.ClassType(Pure.ANY.qualifiedName()), 0), infer("[]").info());
    }

    // ---- if -------------------------------------------------------------

    @Test
    void if_bothBranchesSameType() {
        TypedSpec n = infer("if(true, |1, |2)");
        assertInstanceOf(TypedIf.class, n);
        assertEquals(one(Type.Primitive.INTEGER), n.info());
    }

    @Test
    void if_branchesWidenToCommonSupertype() {
        // |1 (Integer) vs |1.5 (Float) -> Number[1]
        assertEquals(one(Type.Primitive.NUMBER), infer("if(true, |1, |1.5)").info());
    }

    @Test
    void if_nonBooleanConditionThrows() {
        assertThrows(TypeInferenceException.class, () -> infer("if(1, |1, |2)"));
    }

    @Test
    void check_ifExpressionBody() {
        CompiledFunction cf = checkOnly(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { if(lessThanEqual($x, 5), |0, |$x) }",
                "model::f");
        assertEquals(one(Type.Primitive.INTEGER), cf.result().info());
    }

    // ---- let (env threading across statements) -------------------------

    @Test
    void check_letBindingIsInScopeForLaterStatements() {
        CompiledFunction cf = checkOnly(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { let y = $x; $y }", "model::f");
        assertEquals(one(Type.Primitive.INTEGER), cf.result().info());
    }

    @Test
    void check_letWithDifferentlyTypedValue() {
        CompiledFunction cf = checkOnly(
                "function model::f(x:" + INT + "[1]):" + STR + "[1] { let s = 'hi'; $s }", "model::f");
        assertEquals(one(Type.Primitive.STRING), cf.result().info());
    }

    // ---- getAll (query entry point) ------------------------------------

    /** Type-check {@code body} against the Person model with no variables in scope (an ad-hoc query). */
    private static TypedSpec query(String body) {
        return compilerWith(PERSON_MODEL).typeBody(SpecParser.parse(body), Env.empty(), Expected.infer());
    }

    private static final ExprType PERSON_MANY =
            new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ZERO_MANY);

    @Test
    void getAll_returnsClassInstances() {
        // model::Person.all() -> getAll<T>(Class<T>[1]):T[*]  =>  Person[*]
        assertEquals(PERSON_MANY, query("model::Person.all()").info());
    }

    @Test
    void getAll_feedsACollectionFilter() {
        assertEquals(PERSON_MANY,
                query("model::Person.all()->filter(p | lessThanEqual($p.age, 18))").info());
    }

    @Test
    void getAll_feedsExists() {
        assertEquals(one(Type.Primitive.BOOLEAN),
                query("model::Person.all()->exists(p | lessThanEqual($p.age, 18))").info());
    }

    @Test
    void getAll_onNonClassReferenceThrows() {
        assertThrows(TypeInferenceException.class, () -> query("notAClass.all()"));
    }

    // ---- project (relation schema construction) ------------------------

    @Test
    void project_buildsRelationSchemaFromColumns() {
        // model::Person.all()->project([p|$p.name, p|$p.age], ['nm','ag']) : Relation(nm:String[1], ag:Integer[1])[1]
        TypedSpec n = query("model::Person.all()->project([p|$p.name, p|$p.age], ['nm', 'ag'])");
        assertInstanceOf(TypedProject.class, n);
        assertInstanceOf(Type.RelationType.class, n.info().type());

        Type.RelationType rel = (Type.RelationType) n.info().type();
        assertEquals(List.of(
                        new Type.Column("nm", Type.Primitive.STRING, Multiplicity.Bounded.ONE),
                        new Type.Column("ag", Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)),
                rel.columns());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());   // a relation value is [1]
    }

    @Test
    void project_columnFromMultiHopNavigation() {
        // a projected column can be any object-graph expression: $p.home.street : String[0..1]
        TypedSpec n = query("model::Person.all()->project([p|$p.home.street], ['st'])");
        Type.RelationType rel = (Type.RelationType) n.info().type();
        assertEquals(new Type.Column("st", Type.Primitive.STRING, Multiplicity.Bounded.ZERO_ONE),
                rel.columns().get(0));
    }

    @Test
    void project_mismatchedColumnsAndNamesThrows() {
        assertThrows(TypeInferenceException.class,
                () -> query("model::Person.all()->project([p|$p.name], ['a', 'b'])"));
    }

    // ---- scalar & lambda collection ops (no bespoke code needed) -------

    @Test
    void collectionOps_workThroughTheGenericAndLambdaPaths() {
        // These need no per-function checker — they resolve from their signatures.
        assertEquals(one(Type.Primitive.INTEGER), onPerson("$this.friends->size()").info());
        assertEquals(one(Type.Primitive.BOOLEAN), onPerson("$this.friends->isEmpty()").info());
        assertEquals(new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ZERO_ONE),
                onPerson("$this.friends->first()").info());                       // first(T[*]):T[0..1]
        assertEquals(new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ONE),
                onPerson("$this.friends->at(0)").info());                          // at(T[*],Integer):T[1]
        assertEquals(one(Type.Primitive.BOOLEAN),
                onPerson("$this.friends->forAll(f | lessThanEqual($f.age, 5))").info());   // lambda op
    }

    // ---- relation filter (row lambda + column access + G-alpha bridge) --

    @Test
    void relationFilter_rowLambdaAccessesColumnsAndPreservesSchema() {
        // The full query: Person.all() -> project(...) -> filter(r | $r.ag <= 5).
        // filter picks the Relation<T> overload (scored over T[*]); $r.ag is a COLUMN access on the row.
        TypedSpec n = query("model::Person.all()"
                + "->project([p|$p.name, p|$p.age], ['nm', 'ag'])"
                + "->filter(r | lessThanEqual($r.ag, 5))");
        assertInstanceOf(Type.RelationType.class, n.info().type());
        Type.RelationType rel = (Type.RelationType) n.info().type();
        assertEquals(List.of(
                        new Type.Column("nm", Type.Primitive.STRING, Multiplicity.Bounded.ONE),
                        new Type.Column("ag", Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)),
                rel.columns());   // filter preserves the schema
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    @Test
    void relationFilter_unknownColumnInRowLambdaThrows() {
        assertThrows(TypeInferenceException.class, () -> query("model::Person.all()"
                + "->project([p|$p.name], ['nm'])"
                + "->filter(r | lessThanEqual($r.nope, 5))"));
    }

    // ---- lambdas (object-graph collection ops) -------------------------

    @Test
    void lambda_existsOverCollectionReturnsBoolean() {
        // $this.friends->exists(f | lessThanEqual($f.age, 5)) : Boolean[1]; lambda param f : Person[1]
        TypedSpec n = onPerson("$this.friends->exists(f | lessThanEqual($f.age, 5))");
        assertInstanceOf(TypedNativeCall.class, n);
        assertEquals(one(Type.Primitive.BOOLEAN), n.info());
    }

    @Test
    void lambda_filterOverCollectionPreservesElementType() {
        // filter picks the COLLECTION overload (not the Relation one) and returns T[*] = Person[*]
        TypedSpec n = onPerson("$this.friends->filter(f | lessThanEqual($f.age, 5))");
        assertEquals(new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ZERO_MANY),
                n.info());
    }

    @Test
    void lambda_mapInfersResultTypeFromBody() {
        // map<T,V>(T[*], {T->V[*]}):V[*] — V is inferred from the body ($f.name : String) -> String[*]
        TypedSpec n = onPerson("$this.friends->map(f | $f.name)");
        assertEquals(new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_MANY), n.info());
    }

    @Test
    void lambda_bodyClosesOverOuterScope() {
        // the body uses BOTH its own param $f and the outer $this — the lambda env extends the caller's
        TypedSpec n = onPerson("$this.friends->filter(f | lessThanEqual($f.age, $this.age))");
        assertEquals(new ExprType(new Type.ClassType("model::Person"), Multiplicity.Bounded.ZERO_MANY),
                n.info());
    }

    @Test
    void lambda_errorInBodySurfaces() {
        // a bad property access inside the lambda body must fail, not be swallowed
        assertThrows(TypeInferenceException.class,
                () -> onPerson("$this.friends->map(f | $f.notAProperty)"));
    }

    @Test
    void compileReachable_mutualRecursionTerminatesWithNoCycleGuard() {
        // f <-> g : the memo (visited-set) breaks the cycle; each compiles exactly once.
        PureModelContext[] c = new PureModelContext[1];
        SpecCompiler compiler = compilerAndCtx(
                "function model::f(x:" + INT + "[1]):" + INT + "[1] { model::g($x) }\n"
              + "function model::g(x:" + INT + "[1]):" + INT + "[1] { model::f($x) }\n", c);
        TypedFunction f = c[0].findFunction("model::f").get(0);

        var reachable = compiler.compileReachable(f);
        assertEquals(2, reachable.size());   // terminated; each compiled once
        assertEquals(Set.of("model::f", "model::g"), reachable.stream()
                .map(cf -> cf.signature().qualifiedName()).collect(Collectors.toSet()));
    }
}
