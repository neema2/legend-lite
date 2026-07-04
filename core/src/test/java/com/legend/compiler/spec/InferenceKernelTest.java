package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.normalizer.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.parser.ParsedModel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the type-level core of the Phase-G {@link InferenceKernel} &mdash;
 * {@code unify} (solving signature variables from concrete arguments) and
 * {@code resolve} (substituting them back). Covers the rules in
 * PHASE_G_SPEC_COMPILER.md §3.2&ndash;3.3, including the G-&alpha; relation
 * bridge/unwrap and schema algebra.
 */
class InferenceKernelTest {

    private static final String REL = Pure.RELATION.qualifiedName();

    private static PureModelContext ctx() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person {}\nClass model::Address {}\n");
        return PureModelContext.from(new NormalizedModel(parsed.elements(), parsed.imports()));
    }

    private static InferenceKernel kernel() {
        return new InferenceKernel(ctx());
    }

    private static ExprType et(Type t, Multiplicity m) {
        return new ExprType(t, m);
    }

    private static TypedParameter param(Type t, Multiplicity m) {
        return new TypedParameter("p", t, m);
    }

    /** A synthetic native overload with a marker {@link Type.ClassType} return for identity in asserts. */
    private static TypedFunction overload(String returnMarker, List<TypedParameter> params) {
        return new TypedFunction("f", List.of(), List.of(), params,
                new Type.ClassType(returnMarker), Multiplicity.Bounded.ONE, Optional.empty(), true);
    }

    private static Type.Column col(String name, Type type) {
        return new Type.Column(name, type, Multiplicity.Bounded.ONE);
    }

    private static Type.RelationType rel(Type.Column... cols) {
        return new Type.RelationType(List.of(cols));
    }

    // ---- type variables -------------------------------------------------

    @Test
    void typeVar_bindsActualUnchanged_thenResolves() {
        Bindings b = new Bindings();
        kernel().unify(new Type.TypeVar("T"), Type.Primitive.INTEGER, b);
        assertEquals(Type.Primitive.INTEGER, b.type("T").orElseThrow());
        assertEquals(Type.Primitive.INTEGER, kernel().resolve(new Type.TypeVar("T"), b));
    }

    @Test
    void typeVar_conflictingRebindThrows() {
        InferenceKernel k = kernel();
        Bindings b = new Bindings();
        k.unify(new Type.TypeVar("T"), Type.Primitive.INTEGER, b);
        assertThrows(TypeInferenceException.class,
                () -> k.unify(new Type.TypeVar("T"), Type.Primitive.STRING, b));
    }

    @Test
    void typeVar_anyEscapeHatch_keepsOriginalBinding() {
        InferenceKernel k = kernel();
        Bindings b = new Bindings();
        k.unify(new Type.TypeVar("T"), Type.Primitive.INTEGER, b);
        // re-binding to Any is the escape hatch: it does not conflict AND does not overwrite.
        k.unify(new Type.TypeVar("T"), new Type.ClassType(Pure.ANY.qualifiedName()), b);
        assertEquals(Type.Primitive.INTEGER, b.type("T").orElseThrow());
    }

    @Test
    void unboundTypeVarResolutionThrows() {
        assertThrows(TypeInferenceException.class,
                () -> kernel().resolve(new Type.TypeVar("U"), new Bindings()));
    }

    // ---- primitive subtyping (precision/width-agnostic) ----------------

    @Test
    void primitive_subtypeAccepted_supertypeRejected() {
        InferenceKernel k = kernel();
        k.unify(Type.Primitive.NUMBER, Type.Primitive.INTEGER, new Bindings());  // Integer <: Number
        assertThrows(TypeInferenceException.class,
                () -> k.unify(Type.Primitive.INTEGER, Type.Primitive.NUMBER, new Bindings()));
    }

    @Test
    void precisionDecimal_unifiesAsPlainDecimal() {
        InferenceKernel k = kernel();
        // Decimal(10,2) <: Number, and <: Decimal — precision is invisible to unify.
        k.unify(Type.Primitive.NUMBER, new Type.PrecisionDecimal(10, 2), new Bindings());
        k.unify(Type.Primitive.DECIMAL, new Type.PrecisionDecimal(10, 2), new Bindings());
    }

    @Test
    void primitive_siblingsDoNotUnify() {
        InferenceKernel k = kernel();
        // Integer and Decimal/Float are siblings under Number — neither is a subtype of the other.
        assertThrows(TypeInferenceException.class,
                () -> k.unify(Type.Primitive.DECIMAL, Type.Primitive.INTEGER, new Bindings()));
        assertThrows(TypeInferenceException.class,
                () -> k.unify(Type.Primitive.INTEGER, Type.Primitive.FLOAT, new Bindings()));
    }

    @Test
    void enumType_exactFqnOnly() {
        InferenceKernel k = kernel();
        Type color = new Type.EnumType("model::Color");
        k.unify(color, color, new Bindings());
        assertThrows(TypeInferenceException.class,
                () -> k.unify(color, new Type.EnumType("model::Other"), new Bindings()));
        assertThrows(TypeInferenceException.class,
                () -> k.unify(color, Type.Primitive.INTEGER, new Bindings()));
    }

    // ---- non-relation generics -----------------------------------------

    @Test
    void genericNonRelation_recursesArgsAndResolves() {
        InferenceKernel k = kernel();
        Type formal = new Type.GenericType("test::List", List.of(new Type.TypeVar("T")));
        Type actual = new Type.GenericType("test::List", List.of(Type.Primitive.INTEGER));

        Bindings b = new Bindings();
        k.unify(formal, actual, b);
        assertEquals(Type.Primitive.INTEGER, b.type("T").orElseThrow());
        // resolve rebuilds the generic (no relation unwrap) with the substituted arg
        assertEquals(new Type.GenericType("test::List", List.of(Type.Primitive.INTEGER)),
                k.resolve(formal, b));
    }

    @Test
    void genericNonRelation_rawFqnAndNonGenericMismatchThrow() {
        InferenceKernel k = kernel();
        Type formal = new Type.GenericType("test::List", List.of(new Type.TypeVar("T")));
        assertThrows(TypeInferenceException.class, () -> k.unify(formal,
                new Type.GenericType("test::Set", List.of(Type.Primitive.INTEGER)), new Bindings()));
        assertThrows(TypeInferenceException.class,
                () -> k.unify(formal, Type.Primitive.INTEGER, new Bindings()));
    }

    // ---- relation row-struct (column-level) ----------------------------

    @Test
    void relationType_formalColumnsUnifyByName() {
        InferenceKernel k = kernel();
        Type formal = rel(col("x", new Type.TypeVar("T")));   // a formal row with a variable column
        Bindings b = new Bindings();
        k.unify(formal, rel(col("x", Type.Primitive.INTEGER)), b);
        assertEquals(Type.Primitive.INTEGER, b.type("T").orElseThrow());
    }

    @Test
    void relationType_missingColumnThrows() {
        InferenceKernel k = kernel();
        Type formal = rel(col("missing", Type.Primitive.INTEGER));
        assertThrows(TypeInferenceException.class,
                () -> k.unify(formal, rel(col("x", Type.Primitive.INTEGER)), new Bindings()));
    }

    @Test
    void functionTypeParam_isDeferredToTheLambdaPath() {
        Type fn = new Type.FunctionType(
                List.of(new Type.Param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)),
                new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
        assertThrows(TypeInferenceException.class, () -> kernel().unify(fn, fn, new Bindings()));
    }

    // ---- nominal --------------------------------------------------------

    @Test
    void classType_identityOnly_anyIsTop() {
        InferenceKernel k = kernel();
        Type person = new Type.ClassType("model::Person");
        k.unify(person, person, new Bindings());
        assertThrows(TypeInferenceException.class,
                () -> k.unify(person, new Type.ClassType("model::Address"), new Bindings()));
        // Any accepts anything (top type)
        k.unify(new Type.ClassType(Pure.ANY.qualifiedName()), Type.Primitive.INTEGER, new Bindings());
    }

    // ---- relations (G-alpha: bridge + unwrap) --------------------------

    @Test
    void relation_bridgesBareRowAndUnwrapsOnResolve() {
        InferenceKernel k = kernel();
        Type formal = new Type.GenericType(REL, List.of(new Type.TypeVar("T")));
        Type.RelationType row = rel(col("x", Type.Primitive.INTEGER));

        Bindings b = new Bindings();
        k.unify(formal, row, b);                       // actual is a BARE RelationType
        assertEquals(row, b.type("T").orElseThrow());  // schema var bound to the row-struct

        // resolving Relation<T> unwraps to the bare row-struct (the value form)
        assertEquals(row, k.resolve(formal, b));
    }

    @Test
    void relation_bridgesWrappedActualToo() {
        InferenceKernel k = kernel();
        Type formal = new Type.GenericType(REL, List.of(new Type.TypeVar("T")));
        Type.RelationType row = rel(col("x", Type.Primitive.INTEGER));
        Type wrapped = new Type.GenericType(REL, List.of(row));

        Bindings b = new Bindings();
        k.unify(formal, wrapped, b);
        assertEquals(row, b.type("T").orElseThrow());
    }

    @Test
    void relation_nonRelationActualThrows() {
        Type formal = new Type.GenericType(REL, List.of(new Type.TypeVar("T")));
        assertThrows(TypeInferenceException.class,
                () -> kernel().unify(formal, Type.Primitive.INTEGER, new Bindings()));
    }

    // ---- schema algebra -------------------------------------------------

    @Test
    void schemaAlgebra_unionMergesColumns() {
        InferenceKernel k = kernel();
        Bindings b = new Bindings();
        b.bindType("T", rel(col("a", Type.Primitive.INTEGER)));
        b.bindType("V", rel(col("b", Type.Primitive.STRING)));

        Type union = new Type.SchemaAlgebra(new Type.TypeVar("T"), Type.Op.UNION, new Type.TypeVar("V"));
        Type out = k.resolve(union, b);

        assertEquals(rel(col("a", Type.Primitive.INTEGER), col("b", Type.Primitive.STRING)), out);
    }

    @Test
    void schemaAlgebra_differenceDropsNamedColumns() {
        InferenceKernel k = kernel();
        Bindings b = new Bindings();
        b.bindType("T", rel(col("a", Type.Primitive.INTEGER), col("b", Type.Primitive.STRING)));
        b.bindType("Z", rel(col("b", Type.Primitive.STRING)));

        Type diff = new Type.SchemaAlgebra(new Type.TypeVar("T"), Type.Op.DIFFERENCE, new Type.TypeVar("Z"));
        assertEquals(rel(col("a", Type.Primitive.INTEGER)), k.resolve(diff, b));
    }

    // ---- output resolution ----------------------------------------------

    @Test
    void resolveOutput_resolvesTypeAndMultiplicityVar() {
        InferenceKernel k = kernel();
        Bindings b = new Bindings();
        b.bindType("V", Type.Primitive.STRING);
        b.bindMult("m", Multiplicity.Bounded.ZERO_MANY);

        ExprType out = k.resolveOutput(
                new Type.TypeVar("V"), new Multiplicity.Var("m"), b);
        assertEquals(Type.Primitive.STRING, out.type());
        assertEquals(Multiplicity.Bounded.ZERO_MANY, out.multiplicity());
    }

    // ---- overload resolution --------------------------------------------

    @Test
    void overload_noArityMatchThrows() {
        TypedFunction binary = overload("R::Binary",
                List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                        param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertThrows(TypeInferenceException.class, () -> kernel().resolveOverload(
                List.of(binary), List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE))));
    }

    @Test
    void overload_prefersExactOverSubtype() {
        TypedFunction exact = overload("R::Int",
                List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        TypedFunction widened = overload("R::Num",
                List.of(param(Type.Primitive.NUMBER, Multiplicity.Bounded.ONE)));
        InferenceKernel.Resolution r = kernel().resolveOverload(
                List.of(widened, exact),                       // order must not matter
                List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertEquals(new Type.ClassType("R::Int"), r.output().type());
    }

    @Test
    void overload_incomparableSignaturesAreAmbiguous() {
        // f(Integer, Number) and f(Number, Integer) on (Integer, Integer): both score 2+1 — a true tie.
        TypedFunction a = overload("R::A",
                List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                        param(Type.Primitive.NUMBER, Multiplicity.Bounded.ONE)));
        TypedFunction b = overload("R::B",
                List.of(param(Type.Primitive.NUMBER, Multiplicity.Bounded.ONE),
                        param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        List<ExprType> args = List.of(
                et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE));
        assertThrows(TypeInferenceException.class, () -> kernel().resolveOverload(List.of(a, b), args));
    }

    @Test
    void overload_rejectsManyArgIntoToOneParam() {
        TypedFunction toOne = overload("R::One",
                List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertThrows(TypeInferenceException.class, () -> kernel().resolveOverload(
                List.of(toOne), List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ZERO_MANY))));
    }

    @Test
    void overload_realGenericNativeBindsAndResolves() {
        // at<T>(set:T[*], index:Integer[1]):T[1] — resolve at(String[*], Integer[1]) -> String[1].
        PureModelContext ctx = ctx();
        InferenceKernel k = new InferenceKernel(ctx);
        InferenceKernel.Resolution r = k.resolveOverload(
                ctx.findFunction("at"),
                List.of(et(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_MANY),
                        et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertEquals(Type.Primitive.STRING, r.output().type());
        assertEquals(Multiplicity.Bounded.ONE, r.output().multiplicity());
    }

    @Test
    void overload_multipleArityMatchesButNoneFitThrows() {
        // two arity-1 candidates expecting String/Integer, called with a Boolean: no structural match.
        TypedFunction wantsString = overload("R::S", List.of(param(Type.Primitive.STRING, Multiplicity.Bounded.ONE)));
        TypedFunction wantsInt = overload("R::I", List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertThrows(TypeInferenceException.class, () -> kernel().resolveOverload(
                List.of(wantsString, wantsInt), List.of(et(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE))));
    }

    @Test
    void overload_prefersTighterMultiplicity() {
        // same type, different mult: f(Integer[1]) vs f(Integer[*]) on Integer[1] -> [1] wins (exact 5 > [*] tightness 1).
        TypedFunction toOne = overload("R::One", List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        TypedFunction toMany = overload("R::Many", List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ZERO_MANY)));
        InferenceKernel.Resolution r = kernel().resolveOverload(
                List.of(toMany, toOne), List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertEquals(new Type.ClassType("R::One"), r.output().type());
    }

    @Test
    void overload_bindsMultiplicityVariable() {
        // foo<T|m>(x:T[m]):T[m] — foo(String[*]) -> String[*] (binds m=[*] via unifyMult, resolved out).
        TypedFunction foo = new TypedFunction("foo", List.of("T"), List.of("m"),
                List.of(new TypedParameter("x", new Type.TypeVar("T"), new Multiplicity.Var("m"))),
                new Type.TypeVar("T"), new Multiplicity.Var("m"), Optional.empty(), true);
        InferenceKernel.Resolution r = kernel().resolveOverload(
                List.of(foo), List.of(et(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_MANY)));
        assertEquals(Type.Primitive.STRING, r.output().type());
        assertEquals(Multiplicity.Bounded.ZERO_MANY, r.output().multiplicity());
    }

    @Test
    void overload_relationSourceSkipsMultiplicityValidation() {
        // foo<T>(r:Relation<T>[1]):Relation<T>[1] — a relation arg typed [*] must NOT be rejected (§3.2 skip).
        Type relParam = new Type.GenericType(REL, List.of(new Type.TypeVar("T")));
        TypedFunction foo = new TypedFunction("foo", List.of("T"), List.of(),
                List.of(new TypedParameter("r", relParam, Multiplicity.Bounded.ONE)),
                relParam, Multiplicity.Bounded.ONE, Optional.empty(), true);
        Type.RelationType row = rel(col("x", Type.Primitive.INTEGER));
        InferenceKernel.Resolution r = kernel().resolveOverload(
                List.of(foo), List.of(et(row, Multiplicity.Bounded.ZERO_MANY)));   // arg is [*], param is [1]
        assertEquals(row, r.output().type());   // Relation<T> resolves to the bare row-struct
    }

    // ---- resolution error branches --------------------------------------

    @Test
    void resolveMult_unboundVariableThrows() {
        assertThrows(TypeInferenceException.class, () -> kernel().resolveOutput(
                Type.Primitive.STRING, new Multiplicity.Var("m"), new Bindings()));
    }

    @Test
    void schemaAlgebra_leftNotRelationThrows() {
        Bindings b = new Bindings();
        b.bindType("T", Type.Primitive.INTEGER);   // not a relation
        Type sa = new Type.SchemaAlgebra(new Type.TypeVar("T"), Type.Op.UNION, new Type.TypeVar("T"));
        assertThrows(TypeInferenceException.class, () -> kernel().resolve(sa, b));
    }

    @Test
    void schemaAlgebra_unsupportedOperatorThrows() {
        Bindings b = new Bindings();
        b.bindType("T", rel(col("a", Type.Primitive.INTEGER)));
        b.bindType("Z", rel(col("a", Type.Primitive.INTEGER)));
        Type sa = new Type.SchemaAlgebra(new Type.TypeVar("T"), Type.Op.SUBSET, new Type.TypeVar("Z"));
        assertThrows(TypeInferenceException.class, () -> kernel().resolve(sa, b));
    }

    // ---- scoring invariants & integration -------------------------------

    @Test
    void overload_typeSpecificityDominatesMultiplicity() {
        // exact-type + loose-mult (2*10+1 = 21) must beat subtype-type + exact-mult (1*10+5 = 15).
        TypedFunction exactTypeLooseMult = overload("R::ExactType",
                List.of(param(Type.Primitive.INTEGER, Multiplicity.Bounded.ZERO_MANY)));   // Integer[*]
        TypedFunction subTypeExactMult = overload("R::SubType",
                List.of(param(Type.Primitive.NUMBER, Multiplicity.Bounded.ONE)));           // Number[1]
        InferenceKernel.Resolution r = kernel().resolveOverload(
                List.of(subTypeExactMult, exactTypeLooseMult),
                List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertEquals(new Type.ClassType("R::ExactType"), r.output().type());
    }

    @Test
    void overload_typeVarMustBeConsistentAcrossParams() {
        // f<T>(a:T[1], b:T[1]):T[1] — consistent args resolve T; conflicting args throw.
        TypedFunction f = new TypedFunction("f", List.of("T"), List.of(),
                List.of(new TypedParameter("a", new Type.TypeVar("T"), Multiplicity.Bounded.ONE),
                        new TypedParameter("b", new Type.TypeVar("T"), Multiplicity.Bounded.ONE)),
                new Type.TypeVar("T"), Multiplicity.Bounded.ONE, Optional.empty(), true);

        InferenceKernel.Resolution ok = kernel().resolveOverload(List.of(f),
                List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                        et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)));
        assertEquals(Type.Primitive.INTEGER, ok.output().type());

        assertThrows(TypeInferenceException.class, () -> kernel().resolveOverload(List.of(f),
                List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                        et(Type.Primitive.STRING, Multiplicity.Bounded.ONE))));
    }

    @Test
    void overload_emptyCandidatesThrows() {
        assertThrows(TypeInferenceException.class, () -> kernel().resolveOverload(
                List.of(), List.of(et(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE))));
    }

    // =====================================================================
    // Schema-algebra constraints (⊆ / = / fragments) — the generic colspec rules
    // =====================================================================

    private static Type.RelationType fragment(String... names) {
        return new Type.RelationType(java.util.Arrays.stream(names)
                .map(n -> new Type.Column(n, InferenceKernel.UNKNOWN_COLUMN_TYPE, Multiplicity.Bounded.ONE))
                .toList());
    }

    private static Type.RelationType concreteRow() {
        return new Type.RelationType(List.of(
                new Type.Column("A", Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
                new Type.Column("B", Type.Primitive.STRING, new Multiplicity.Bounded(0, 1))));
    }

    @Test
    void subsetSelectsConcreteColumnsByName() {
        Bindings b = new Bindings();
        b.bindType("T", concreteRow());
        Type formal = new Type.SchemaAlgebra(new Type.TypeVar("X"), Type.Op.SUBSET, new Type.TypeVar("T"));
        kernel().unify(formal, fragment("B"), b);
        Type.RelationType x = (Type.RelationType) b.type("X").orElseThrow();
        assertEquals(1, x.columns().size());
        assertEquals(Type.Primitive.STRING, x.columns().get(0).type());
        assertEquals(new Multiplicity.Bounded(0, 1), x.columns().get(0).multiplicity(),
                "⊆ selection carries the concrete column's multiplicity");
    }

    @Test
    void subsetUnknownColumnThrows() {
        Bindings b = new Bindings();
        b.bindType("T", concreteRow());
        Type formal = new Type.SchemaAlgebra(new Type.TypeVar("X"), Type.Op.SUBSET, new Type.TypeVar("T"));
        assertThrows(TypeInferenceException.class, () -> kernel().unify(formal, fragment("NOPE"), b));
    }

    @Test
    void subsetAccumulatesAcrossKeys() {
        // SortInfo<X⊆T>[*]: each key unifies separately; X unions.
        Bindings b = new Bindings();
        b.bindType("T", concreteRow());
        Type formal = new Type.SchemaAlgebra(new Type.TypeVar("X"), Type.Op.SUBSET, new Type.TypeVar("T"));
        kernel().unify(formal, fragment("A"), b);
        kernel().unify(formal, fragment("B"), b);
        Type.RelationType x = (Type.RelationType) b.type("X").orElseThrow();
        assertEquals(List.of("A", "B"), x.columns().stream().map(Type.Column::name).toList());
    }

    @Test
    void wildcardEqualCarriesTypeAndShadowMultAcrossK() {
        // rename's two sides: Z=(?:K)⊆T binds K (+ shadow mult) from the old column;
        // V=(?:K) concretizes the new column from K.
        Bindings b = new Bindings();
        b.bindType("T", concreteRow());
        Type.RelationType wild = new Type.RelationType(List.of(
                new Type.Column("?", new Type.TypeVar("K"), Multiplicity.Bounded.ONE)));
        Type oldSide = new Type.SchemaAlgebra(
                new Type.SchemaAlgebra(new Type.TypeVar("Z"), Type.Op.EQUAL, wild),
                Type.Op.SUBSET, new Type.TypeVar("T"));
        kernel().unify(oldSide, fragment("B"), b);
        assertEquals(Type.Primitive.STRING, b.type("K").orElseThrow());

        Type newSide = new Type.SchemaAlgebra(new Type.TypeVar("V"), Type.Op.EQUAL, wild);
        kernel().unify(newSide, fragment("NEW"), b);
        Type.RelationType v = (Type.RelationType) b.type("V").orElseThrow();
        assertEquals("NEW", v.columns().get(0).name());
        assertEquals(Type.Primitive.STRING, v.columns().get(0).type(), "K carries the type");
        assertEquals(new Multiplicity.Bounded(0, 1), v.columns().get(0).multiplicity(),
                "the shadow ?K binding carries the multiplicity");
    }

    @Test
    void unsolvedFragmentsMergeAtSharedVariable() {
        // over<T>(ColSpec<T>, SortInfo<T>[*]): partition and sort fragments union at T.
        Bindings b = new Bindings();
        kernel().unify(new Type.TypeVar("T"), fragment("A"), b);
        kernel().unify(new Type.TypeVar("T"), fragment("B"), b);
        Type.RelationType t = (Type.RelationType) b.type("T").orElseThrow();
        assertEquals(List.of("A", "B"), t.columns().stream().map(Type.Column::name).toList());
    }

    @Test
    void fragmentRebindAgainstConcreteRowValidatesContainment() {
        // extend's _Window<T> with T bound to the source: the window fragment's names
        // must exist there — that check IS the partition-column validation.
        Bindings b = new Bindings();
        b.bindType("T", concreteRow());
        kernel().unify(new Type.TypeVar("T"), fragment("A"), b);   // contained: OK, keeps binding
        assertEquals(concreteRow(), b.type("T").orElseThrow());
        assertThrows(TypeInferenceException.class,
                () -> kernel().unify(new Type.TypeVar("T"), fragment("NOPE"), b));
    }

    @Test
    void unionCollisionFailsLoudly() {
        // resolve(T+Z) with overlapping names — real legend-pure errors, never silent.
        Bindings b = new Bindings();
        b.bindType("T", concreteRow());
        b.bindType("Z", new Type.RelationType(List.of(
                new Type.Column("A", Type.Primitive.FLOAT, Multiplicity.Bounded.ONE))));
        Type union = new Type.SchemaAlgebra(new Type.TypeVar("T"), Type.Op.UNION, new Type.TypeVar("Z"));
        assertThrows(TypeInferenceException.class, () -> kernel().resolve(union, b));
    }

    @Test
    void bindingsCopyIsIsolated() {
        // AggColSpecArray's K/V solve per column: a copy sees prior bindings but
        // its new ones never leak back.
        Bindings parent = new Bindings();
        parent.bindType("T", Type.Primitive.INTEGER);
        Bindings child = parent.copy();
        child.bindType("K", Type.Primitive.STRING);
        assertEquals(Type.Primitive.INTEGER, child.type("T").orElseThrow());
        assertTrue(parent.type("K").isEmpty(), "child bindings must not leak to the parent");
    }

    @Test
    void unknownColumnTypePassesThroughResolveUnbound() {
        Type row = fragment("A");
        assertEquals(row, kernel().resolve(row, new Bindings()),
                "`?` is a placeholder, not a solvable variable");
    }

}
