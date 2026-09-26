package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedClass;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The shared, stateless type-inference machinery for Phase G (engine
 * {@code AbstractChecker}, the framework half). Holds only a {@link ModelContext}
 * (for subtyping); every operation is a pure function of its arguments
 * (PHASE_G_SPEC_COMPILER.md §3).
 *
 * <p>This class is the <em>type-level</em> core: {@link #unify} solves a
 * signature's variables from concrete argument types, and {@link #resolve} /
 * {@link #resolveOutput} substitute them back to compute a return type. Overload
 * scoring and the bidirectional body checker build on top of this and live
 * elsewhere.
 *
 * <h2>Relation representation (reference-faithful; ex-G-&alpha;)</h2>
 * A TABLE value's type is {@code GenericType(Relation, [schema])} &mdash; pure's
 * own signature spelling, preserved through resolution (the historical G-&alpha;
 * erasure to a bare struct is deleted). A bare {@link Type.RelationType} is the
 * SCHEMA STRUCT and, as a value type, ONE ROW &mdash; pure's pun: the {@code T}
 * of {@code Relation<T>} is the schema AND the row type. {@link #unify} binds
 * the schema variable to the bare struct; {@link #resolve} substitutes it back
 * under the wrapper untouched.
 */
public final class InferenceKernel {

    private static final String RELATION_FQN = com.legend.compiler.element.type.PlatformTypes.RELATION;
    private static final String ANY_FQN = com.legend.compiler.element.type.PlatformTypes.ANY;
    private static final String NIL_FQN = com.legend.compiler.element.type.PlatformTypes.NIL;

    private final ModelContext ctx;

    public InferenceKernel(ModelContext ctx) {
        this.ctx = ctx;
    }

    // =====================================================================
    // Unification &mdash; solve the variables in `formal` from concrete `actual`
    // =====================================================================

    /**
     * Unify a signature parameter type {@code formal} (which may contain type
     * variables) against a concrete argument type {@code actual}, accumulating
     * solved variables into {@code b}. Throws {@link TypeInferenceException} on
     * an unsatisfiable constraint.
     */
    /** The m3 Enum metaclass — classified as an enumeration-VALUE type
     * (TypeClassifier), never a class navigation. */
    public static final String ENUM_METACLASS_FQN = "meta::pure::metamodel::type::Enum";

    /** The parameterized-class arm of {@link #unify}: nominal on the raw
     * class with the lattice; a subclass actual's arguments pair as the
     * formal's raw class sees them (asSuper); a Nil formal argument is the
     * wildcard. */
    private void unifyGeneric(Type.GenericType g, Type formal, Type actual, Bindings b) {

                // Nominal on the raw class, with the class lattice — a
                // parameterized SUBCLASS actual conforms (m3's function
                // carriers: LambdaFunction<{…}> flows into a
                // FunctionDefinition<Any> formal), mirroring the ClassType
                // arm's isSubtype rule.
                if (!(actual instanceof Type.GenericType ag0
                        && (ag0.rawFqn().equals(g.rawFqn())
                                || ctx.isSubtype(ag0.rawFqn(), g.rawFqn())))) {
                    throw fail(formal, actual);
                }
                // a SUBCLASS actual pairs arguments as the formal's raw class
                // sees them: its declared supertypes instantiated (asSuper —
                // Property<U,V|m> is AbstractProperty<{U[1]->V[m]}>), never
                // positionally across unrelated parameter lists
                Type.GenericType ag = ag0.rawFqn().equals(g.rawFqn()) ? ag0
                        : asSuper(ag0, g.rawFqn()).filter(t -> t instanceof Type.GenericType)
                                .map(t -> (Type.GenericType) t).orElse(ag0);
                if (ag.arguments().size() != g.arguments().size()) {
                    throw fail(formal, actual);
                }
                for (int i = 0; i < g.arguments().size(); i++) {
                    // a Nil type ARGUMENT in the formal is real pure's
                    // wildcard (Property<Nil,Any|*> takes any property)
                    if (isNil(g.arguments().get(i))) {
                        continue;
                    }
                    unify(g.arguments().get(i), ag.arguments().get(i), b);
                }
    }

    public void unify(Type formal, Type actual, Bindings b) {
        // Function<{...}> is the WRAPPED spelling of a bare FunctionType —
        // signatures use the wrapper, function VALUES carry a carrier
        // (LambdaFunction<{…}> etc.). Normalization is PAIRWISE: when the
        // formal is (or unwraps to) a structural FunctionType the actual
        // unwraps with it; a formal that KEEPS its carrier nominal
        // (FunctionDefinition<Any> — its argument is no FunctionType) must
        // see the actual's carrier too, so the nominal lattice can judge
        // (LambdaFunction ≤ FunctionDefinition; a Function<Any> ref is NOT).
        Type ff = unwrapFunction(formal);
        // an Any formal keeps the carrier too: Any accepts a function VALUE
        // as a value (real pure: size(Any[*]) over a lambda collection counts
        // the lambdas — batch 5, upstream's size/count text)
        boolean formalKeepsCarrier = ff == formal
                && ((formal instanceof Type.GenericType fg
                        && FUNCTION_CARRIER_FQNS.contains(fg.rawFqn()))
                    || PlatformTypes.isAny(formal));
        // NOMINAL GATE before any structural unwrap (audit 2026-08-28
        // R5): when BOTH sides are carriers the class lattice judges
        // FIRST — a Function-carrier actual must not slip into a
        // LambdaFunction<{sig}> formal just because the signatures line
        // up. A bare-FunctionType side imposes/carries no nominal
        // constraint and keeps the structural path.
        if (formal instanceof Type.GenericType nf2
                && FUNCTION_CARRIER_FQNS.contains(nf2.rawFqn())
                && actual instanceof Type.GenericType na2
                && FUNCTION_CARRIER_FQNS.contains(na2.rawFqn())
                && !na2.rawFqn().equals(nf2.rawFqn())
                && !ctx.isSubtype(na2.rawFqn(), nf2.rawFqn())) {
            throw fail(formal, actual);
        }
        Type fa = formalKeepsCarrier ? actual : unwrapFunctionValue(actual, ff);
        if (ff != formal || fa != actual) {
            unify(ff, fa, b);
            return;
        }
        // Nil is BOTTOM (the []-born value): it conforms to EVERY formal —
        // class and generic slots included (executeLegendQuery($f, [],
        // ext) against vars:Pair<String, Any>[*]; the multiplicity check
        // is the one that judges an empty). The primitive and type-
        // variable arms below carry the same rule for their own shapes.
        if (isNil(actual) && !(formal instanceof Type.TypeVar)) {
            return;
        }
        switch (formal) {
            // Any is the top type: accepts anything (guarded ClassType, before identity).
            case Type.ClassType c when c.fqn().equals(ANY_FQN) -> { }

            // The unknown column type `?` (a colspec value's placeholder) accepts anything
            // and solves nothing — it must never be bound as a real variable.
            case Type.TypeVar v when isUnknown(v) -> { }
            case Type.TypeVar v -> bindOrCheckTypeVar(v, actual, b);

            // Primitive lattice (precision/width-agnostic).
            case Type.Primitive ignored -> requirePrimitiveSubtype(actual, formal);
            case Type.PrecisionDecimal ignored -> requirePrimitiveSubtype(actual, formal);

            case Type.EnumType e -> {
                // the m3 Enum METACLASS in value position is "some enumeration
                // value" (EnumValueMapping.enum): any enumeration's value
                // conforms, and so does its name carrier (the system store's
                // enum_value column — an enum value IS its name here)
                boolean metaclass = e.fqn().equals(ENUM_METACLASS_FQN)
                        && (actual instanceof Type.EnumType || actual == Type.Primitive.STRING);
                if (!metaclass && !(actual instanceof Type.EnumType ae && ae.fqn().equals(e.fqn()))) {
                    throw fail(formal, actual);
                }
            }
            // TabularDataSet in CLASS-TYPE spelling (resolved through the
            // corpus module's own m3 class): same schema-erasing nominal.
            case Type.ClassType c
                    when c.fqn().equals(PlatformTypes.TABULAR_DATA_SET)
                    && Type.isRelation(actual) -> { }
            case Type.ClassType c -> {
                // SUBTYPE conformance (a Person flows into an Employee-typed
                // param's superclass) — matching what overload SCORING already
                // accepts (paramTypeScore scores subtypes as matches); the two
                // halves of one kernel must agree, or resolution selects a
                // winner unification then rejects (audit finding).
                boolean nominalOk = actual instanceof Type.ClassType ac
                        && (ac.fqn().equals(c.fqn()) || ctx.isSubtype(ac.fqn(), c.fqn()));
                // a PARAMETERIZED actual conforms to a raw class formal by
                // erasure (Class<TestClass> flows into ModelElement — real
                // pure; the m3 hierarchy is nominal on the raw type)
                boolean erasureOk = actual instanceof Type.GenericType ag
                        && (ag.rawFqn().equals(c.fqn()) || ctx.isSubtype(ag.rawFqn(), c.fqn()));
                if (!nominalOk && !erasureOk) {
                    throw fail(formal, actual);
                }
            }

            // Dedicated relation case (Row-vs-Relation): a table actual
            // is WRAPPED — the schema variable binds the bare schema
            // struct inside it (pure's T = the schema = the row type).
            // A bare struct actual is a ROW and does NOT conform to a
            // Relation<T> formal (the type distinction the split
            // exists to enforce).
            case Type.GenericType g when g.rawFqn().equals(RELATION_FQN) -> {
                if (!(actual instanceof Type.GenericType ag
                        && (ag.rawFqn().equals(RELATION_FQN)
                                || ctx.isSubtype(ag.rawFqn(), RELATION_FQN))
                        && ag.arguments().size() == 1)) {
                    throw new TypeInferenceException("expected a Relation, got " + actual.typeName());
                }
                unify(g.arguments().get(0), ag.arguments().get(0), b);
            }
            // TabularDataSet is the SCHEMA-ERASING nominal over the relation
            // carrier (CastChecker's cast(@TabularDataSet) doctrine): any
            // relation-shaped actual conforms (a corpus function declared
            // over TDS receives the platform's typed row-struct).
            case Type.GenericType g
                    when g.rawFqn().equals(PlatformTypes.TABULAR_DATA_SET)
                    && Type.isRelation(actual) -> { }
            // Function<Any> is the universal function bound (real m3:
            // every function instance is a Function; Any covers all
            // function types) — a bare FunctionType actual conforms
            // (generateUsageFor metadata holding eta-expanded refs)
            case Type.GenericType g
                    when g.rawFqn().equals(
                            "meta::pure::metamodel::function::Function")
                    && g.arguments().size() == 1
                    && PlatformTypes.isAny(g.arguments().get(0))
                    && actual instanceof Type.FunctionType -> { }
            // a parameterized FORMAL against a non-generic actual (a class
            // row / a raw class instance — the system store's Class<Any>
            // and Property<Nil,Any> ends): conformance is the raw class's
            // (the real m3 declarations instantiate what the hand copies
            // wrote raw; the store's rows are the same rows)
            case Type.GenericType g when !(actual instanceof Type.GenericType)
                    && !g.rawFqn().equals(RELATION_FQN)
                    && !Type.isRelation(actual) ->
                    unify(new Type.ClassType(g.rawFqn()), actual, b);
            case Type.GenericType g -> unifyGeneric(g, formal, actual, b);

            // A bare STRUCT formal (a declared inline row/schema param,
            // or a colspec row): unify by columns against the actual's
            // schema view (a row's own struct, or a table's schema —
            // struct-spelled table params are legacy-tolerated).
            case Type.RelationType r -> {
                if (!(Type.schemaView(actual) instanceof Type.RelationType ar)) {
                    throw fail(formal, actual);
                }
                unifyColumns(r, ar, b);
            }

            // A schema-algebra CONSTRAINT in parameter position (`Z⊆T`, `Z=(?:K)⊆T`,
            // `V=(?:K)` inside ColSpec/SortInfo/… generics) — evaluated against the
            // actual's row-struct. Algebra in RETURN position (`T-Z+V`) is evaluated
            // by resolve(), not here.
            case Type.SchemaAlgebra sa -> unifyConstraint(sa, actual, b);

            // FUNCTION-TYPE unification (the eval/match keystone): a formal
            // Function<{T[n]->V[m]}> against an actual function VALUE's type.
            // Params are CONTRAVARIANT — a formal Nil param (real pure's
            // match branches: Function<{Nil[n]->T[m]}>) is the bottom type
            // and accepts any actual param. Lambda LITERALS never reach here
            // (they defer and type against the expected signature); this arm
            // serves function values — variables, colspec functions, refs.
            case Type.FunctionType f -> {
                if (!(actual instanceof Type.FunctionType af)) {
                    throw fail(formal, actual);
                }
                if (af.params().size() != f.params().size()) {
                    throw new TypeInferenceException("function shape mismatch: expected "
                            + f.params().size() + " parameter(s), got " + af.params().size()
                            + " (" + actual.typeName() + ")");
                }
                b.enterContravariant();
                try {
                    for (int i = 0; i < f.params().size(); i++) {
                        Type formalParam = f.params().get(i).type();
                        if (formalParam instanceof Type.ClassType c && c.fqn().equals(NIL_FQN)) {
                            continue;   // bottom type: any actual param conforms
                        }
                        // CONTRAVARIANT parameter positions (DEEP_AUDIT
                        // §5b; reference TypeMatch matches params with
                        // !covariant): the ACTUAL's param must be a
                        // SUPERTYPE of the formal's — arguments SWAP.
                        // The old covariant order both wrong-ACCEPTED
                        // (an Integer[1] lambda receiving 1.5) and
                        // wrong-REJECTED (Employee[*]->map(Person-fn)).
                        // An UNBOUND formal type/mult VARIABLE keeps the
                        // binding order — variance is moot for a fresh
                        // var and the swap would move it to the
                        // non-binding side.
                        if (formalParam instanceof Type.TypeVar) {
                            unify(formalParam, af.params().get(i).type(), b);
                        } else {
                            unify(af.params().get(i).type(), formalParam, b);
                        }
                        // the mult check FLIPS instead of skipping
                        // (formal range must sit INSIDE the actual's —
                        // still admits the equal(Any[*],Any[*])
                        // comparator doctrine, [1] ⊆ [*]; rejects a
                        // [2..3]-taking actual in a [1] slot)
                        if (f.params().get(i).multiplicity()
                                instanceof Multiplicity.Var) {
                            unifyMult(f.params().get(i).multiplicity(),
                                    af.params().get(i).multiplicity(),
                                    af.params().get(i).type(), b, true);
                        } else {
                            unifyMult(af.params().get(i).multiplicity(),
                                    f.params().get(i).multiplicity(),
                                    f.params().get(i).type(), b, false);
                        }
                    }
                } finally {
                    b.exitContravariant();
                }
                unify(f.result().type(), af.result().type(), b);
                // FUNCTION-VALUE RESULT slots are lenient on the LOWER
                // bound: the engine's own corpus compiles sortBy over
                // optional association paths — a {T[1]->String[0..1]}
                // key against sortBy's declared {T[1]->U[1]} (the
                // reference's observed lambda-result covariance; the
                // UPPER bound stays checked — a [*] result cannot take
                // a to-one slot, the earlier audit's interior-result
                // fix). VALUE slots keep full strict containment.
                unifyMultResult(f.result().multiplicity(),
                        af.result().multiplicity(), af.result().type(), b);
            }
        }
    }

    /** Result-slot multiplicity conformance: upper bound only (see the
     * FunctionType arm — engine-observed lambda-result covariance; the
     * Typer's lambda-LITERAL body check routes here too). */
    void unifyMultResult(Multiplicity formal, Multiplicity actual,
            Type actualType, Bindings b) {
        if (formal instanceof Multiplicity.Var) {
            unifyMult(formal, actual, actualType, b);
            return;
        }
        if (formal instanceof Multiplicity.Bounded fb
                && actual instanceof Multiplicity.Bounded ab
                && !Type.isRelation(actualType)
                && !com.legend.compiler.element.type.PlatformTypes
                        .isVariant(actualType)) {
            boolean upperOk = fb.upper() == null
                    || (ab.upper() != null && ab.upper() <= fb.upper());
            if (!upperOk) {
                throw new TypeInferenceException("multiplicity " + ab.text()
                        + " is not compatible with result " + fb.text());
            }
        }
    }

    // =====================================================================
    // Schema-algebra constraints (the generic colspec rules)
    // =====================================================================

    /**
     * Evaluate a schema-algebra <em>constraint</em> from a signature parameter
     * against an actual row-struct (the row of a colspec value, whose column
     * types are the {@link #isUnknown unknown} {@code ?} until solved here):
     *
     * <ul>
     *   <li>{@code X ⊆ T} &mdash; the actual's column <strong>names</strong> select
     *       concrete columns from the (already bound) {@code T}; a missing name is
     *       the "unknown column" error. {@code X} binds to the concrete selection,
     *       <em>accumulating</em> by union on rebind (so {@code SortInfo<X⊆T>[*]}
     *       collects every sort key into one {@code X}).</li>
     *   <li>{@code X = (?:K)} &mdash; a single-column wildcard shape: {@code K}
     *       binds the column's type when unknown ({@code rename}'s old side, where
     *       {@code ⊆} has already concretized it), or concretizes the actual's
     *       {@code ?} when already bound (the new side) &mdash; that shared {@code K}
     *       is how rename preserves a column's type with zero bespoke code. The
     *       column's <em>multiplicity</em> rides a shadow binding ({@code ?K}): the
     *       algebra's {@code K} carries only the type, but a renamed {@code [0..1]}
     *       column must stay {@code [0..1]}.</li>
     * </ul>
     */
    private void unifyConstraint(Type.SchemaAlgebra sa, Type actual, Bindings b) {
        // a GENERIC caller passing its OWN constrained value: the constraint is
        // still symbolic on both sides (core_functions_relation reduce<T,V,U,X>'s
        // sortInfo:SortInfo<X⊆T>[*] handed to sort<X,T>) — the two constraints
        // unify side by side, never against a concrete row
        if (actual instanceof Type.SchemaAlgebra asa && asa.op() == sa.op()) {
            unify(sa.left(), asa.left(), b);
            unify(sa.right(), asa.right(), b);
            return;
        }
        if (!(Type.schemaView(actual) instanceof Type.RelationType actualRow)) {
            throw new TypeInferenceException("expected a column specification (a row-struct), got "
                    + actual.typeName());
        }
        switch (sa.op()) {
            case SUBSET -> {
                // the superset variable is bound by a PARAMETER (rename's
                // Relation<T>) or by the CALLER'S EXPECTED TYPE (over<T>(cols:
                // ColSpec<(?:?)⊆T>) inside extend: real pure's bidirectional
                // inference — resolveOverload seeds T from the expected _Window<T>)
                Type right = resolve(sa.right(), b);
                if (!(right instanceof Type.RelationType schema)) {
                    throw new TypeInferenceException("⊆ right-hand side is not a relation schema: "
                            + right.typeName());
                }
                List<Type.Column> selected = new ArrayList<>(actualRow.columns().size());
                for (Type.Column c : actualRow.columns()) {
                    selected.add(schema.columns().stream()
                            .filter(sc -> sameColumn(sc.name(), c.name()))
                            .findFirst()
                            .orElseThrow(() -> new TypeInferenceException(
                                    "unknown column '" + c.name() + "' in " + schema.typeName())));
                }
                unifyConstraintLeft(sa.left(), new Type.RelationType(selected), b);
            }
            case EQUAL -> unifyWildcardEqual(sa, actualRow, b);
            default -> throw new TypeInferenceException(
                    "schema-algebra operator " + sa.op() + " is not a parameter constraint");
        }
    }

    /**
     * The left side of a {@code ⊆}: a plain variable, a nested {@code X=(?:K)}
     * shape, or a bare wildcard row {@code (?:K)} (real pure's
     * {@code ColSpec<(?:Number)⊆T>} — the selected column may have ANY name
     * but its type must conform to {@code K}).
     */
    private void unifyConstraintLeft(Type left, Type.RelationType concrete, Bindings b) {
        switch (left) {
            case Type.TypeVar v -> bindRowAccumulating(v, concrete, b);
            case Type.SchemaAlgebra eq when eq.op() == Type.Op.EQUAL -> unifyWildcardEqual(eq, concrete, b);
            case Type.RelationType wildcard when wildcard.columns().size() == 1
                    && wildcard.columns().get(0).name().equals("?") -> {
                // a wildcard row admits ONE column under ColSpec and MANY under
                // ColSpecArray (upstream's over(cols:ColSpecArray<(?:?)⊆T>)) —
                // the carrier fixes the count, the constraint types each column
                Type want = wildcard.columns().get(0).type();
                for (Type.Column c : concrete.columns()) {
                    Type got = c.type();
                    if (!conformsForWildcard(got, want)) {
                        throw new TypeInferenceException("column '" + c.name()
                                + "' has type " + got.typeName()
                                + " but the constraint requires " + want.typeName());
                    }
                }
            }
            default -> throw new TypeInferenceException(
                    "unsupported ⊆ left-hand side: " + left.typeName());
        }
    }

    /** Wildcard-column type conformance: exact, or within the numeric family. */
    private boolean conformsForWildcard(Type got, Type want) {
        // (?:?) — the column's TYPE is the wildcard too (upstream's over /
        // rename / variantFlatten spellings): any type conforms
        if (want instanceof Type.TypeVar wv && wv.name().equals("?")) {
            return true;
        }
        if (got.equals(want)) {
            return true;
        }
        if (want == Type.Primitive.NUMBER && got instanceof Type.Primitive p) {
            return p.family() == Type.Primitive.Family.NUMERIC;
        }
        return got instanceof Type.PrecisionDecimal && want == Type.Primitive.NUMBER;
    }

    /**
     * {@code X = (?:K)}: match a single-column wildcard row against a single-column
     * actual, solving {@code K} (and its shadow multiplicity), then bind {@code X}
     * to the concretized row.
     */
    private void unifyWildcardEqual(Type.SchemaAlgebra eq, Type.RelationType actualRow, Bindings b) {
        if (!(eq.left() instanceof Type.TypeVar v)) {
            throw new TypeInferenceException("= left-hand side must be a schema variable, got "
                    + eq.left().typeName());
        }
        if (!(eq.right() instanceof Type.RelationType wild) || wild.columns().size() != 1
                || actualRow.columns().size() != 1) {
            throw new TypeInferenceException(
                    "column wildcard '=' expects single-column shapes; got " + eq.right().typeName()
                            + " against " + actualRow.typeName());
        }
        Type.Column actual = actualRow.columns().get(0);
        Type wildType = wild.columns().get(0).type();

        Type colType = actual.type();
        Multiplicity colMult = actual.multiplicity();
        if (wildType instanceof Type.TypeVar k && !isUnknown(k)) {
            if (isUnknown(colType)) {
                // The new side (`V=(?:K)`): the actual's type is the unsolved `?` — K carries it over.
                colType = b.type(k.name()).orElseThrow(() -> new TypeInferenceException(
                        "unbound column-type variable " + k.name()));
                colMult = b.mult(shadowMult(k)).orElse(colMult);
            } else {
                // The old side (`Z=(?:K)⊆T`, already concretized by ⊆): K learns the column's type.
                bindOrCheckTypeVar(k, colType, b);
                b.bindMult(shadowMult(k), colMult);
            }
        } else if (isUnknown(colType)) {
            throw new TypeInferenceException("cannot solve the type of column '" + actual.name() + "'");
        }
        bindRowAccumulating(v, new Type.RelationType(
                List.of(new Type.Column(actual.name(), colType, colMult))), b);
    }

    /** The shadow multiplicity slot of a column-type variable {@code K} (see {@link #unifyWildcardEqual}). */
    private static String shadowMult(Type.TypeVar k) {
        return "?" + k.name();
    }

    /**
     * Bind a schema variable to a concrete row, <em>unioning</em> on rebind
     * (subset constraints accumulate; a same-named column must agree).
     */
    private void bindRowAccumulating(Type.TypeVar v, Type.RelationType row, Bindings b) {
        Type existing = b.type(v.name()).orElse(null);
        if (existing == null) {
            b.bindType(v.name(), row);
            return;
        }
        if (!(existing instanceof Type.RelationType er)) {
            throw new TypeInferenceException("schema variable " + v.name()
                    + " is already bound to a non-relation: " + existing.typeName());
        }
        List<Type.Column> merged = new ArrayList<>(er.columns());
        for (Type.Column c : row.columns()) {
            Type.Column prior = merged.stream().filter(m -> sameColumn(m.name(), c.name())).findFirst().orElse(null);
            if (prior == null) {
                merged.add(c);
            } else if (!prior.equals(c)) {
                throw new TypeInferenceException("schema variable " + v.name()
                        + " binds column '" + c.name() + "' inconsistently");
            }
        }
        b.bindType(v.name(), new Type.RelationType(merged));
    }

    /** Column-name IDENTITY: a QUOTE-BEARING spelling ('"FIRST NAME"' —
     * quoted store declaration) and its stripped text are the SAME
     * column; the quotes are rendering metadata. */
    static boolean sameColumn(String a, String b) {
        return stripColQ(a).equals(stripColQ(b));
    }

    private static String stripColQ(String n) {
        return n.length() > 1 && n.startsWith("\"") && n.endsWith("\"")
                ? n.substring(1, n.length() - 1) : n;
    }

    /** The distinguished unknown column type of a not-yet-solved colspec value ({@code ~col}). */
    public static boolean isUnknown(Type t) {
        return t instanceof Type.TypeVar v && v.name().equals(UNKNOWN_COLUMN_TYPE.name());
    }

    /** The {@code ?} type a bare colspec value carries per column until {@code ⊆}/{@code =} solves it. */
    public static final Type.TypeVar UNKNOWN_COLUMN_TYPE = new Type.TypeVar("?");

    private void bindOrCheckTypeVar(Type.TypeVar v, Type actual, Bindings b) {
        if (b.hasType(v.name())) {
            Type existing = b.type(v.name()).orElseThrow();
            // Two UNSOLVED schema fragments meeting at the same variable MERGE — like ⊆
            // accumulation: over<T>(ColSpec<T>, SortInfo<T>[*]) collects the partition and
            // sort columns into one fragment, later validated against the extend source.
            if (existing instanceof Type.RelationType er && isUnknownFragment(er)
                    && actual instanceof Type.RelationType ar && isUnknownFragment(ar)) {
                b.bindType(v.name(), unionRows(er, ar));
                return;
            }
            // Nil is BOTTOM (the []-born element type): it conforms to every
            // binding and never constrains one — the other side wins, the same
            // rule that makes Nil vanish in collection-literal LUBs. This is how
            // coalesce<T>([], 'x') binds T=String (real pure's covariant binding).
            if (isNil(actual)) {
                return;
            }
            if (isNil(existing)) {
                b.bindType(v.name(), actual);
                return;
            }
            // COVARIANT class binding (real pure's getBestGenericTypeUsingCovariance):
            // two INSTANCE kinds meet at their least common ancestor —
            // concatenate(CO_Address[*], CO_Location[*]) binds T to their
            // shared CO_GeographicEntity. A RIGID variable (bound while
            // unifying a declared function-parameter type) never widens:
            // the actual must CONFORM — subtype in, or loud (the
            // eval-wrong-arg engine spec, class edition; audit).
            if (existing instanceof Type.ClassType ec && actual instanceof Type.ClassType ac
                    && !isAny(existing) && !isAny(actual)) {
                if (b.isRigid(v.name()) || b.contravariant()) {
                    if (ac.fqn().equals(ec.fqn()) || ctx.isSubtype(ac.fqn(), ec.fqn())) {
                        return;
                    }
                    throw new TypeInferenceException("type variable " + v.name()
                            + " is bound to " + existing.typeName()
                            + " (a declared function parameter) and cannot accept "
                            + actual.typeName());
                }
                b.bindType(v.name(), commonSupertype(existing, actual));
                return;
            }
            // The same covariance for SAME-RAW parameterized classes: real
            // pure joins them ARG-WISE (Pair<Function<Any>, {->SchemaState}>
            // meets Pair<{Table->TableTDS}, {->…}> in the engine's extension
            // record — Phase 5 batch 147). Relation-schema containers stay
            // loud below: a relation's identity is its columns.
            if (existing instanceof Type.GenericType eg && actual instanceof Type.GenericType ag
                    && eg.rawFqn().equals(ag.rawFqn())
                    && eg.arguments().size() == ag.arguments().size()
                    && !b.isRigid(v.name()) && !b.contravariant()
                    && Type.schemaView(existing) == null && Type.schemaView(actual) == null) {
                b.bindType(v.name(), commonSupertype(existing, actual));
                return;
            }
            // A variable already bound to a FUNCTION TYPE meeting another function
            // type UNIFIES structurally instead of demanding equality: the
            // existing binding may carry the ENCLOSING function's own type and
            // multiplicity parameters (the PCT harness shape `f:Function<{Function<
            // {->Z[y]}>[1]->Z[y]}>`, then `$f->eval(|1)`), and real pure binds those
            // per expression — Z := Integer, y := 1 for THIS call. A genuinely
            // different function type still fails inside unify (the eval-wrong-arg
            // spec). Typing census 2026-09-08: 605 of 643 rows were this.
            if (existing instanceof Type.FunctionType ef && actual instanceof Type.FunctionType af
                    && ef.params().size() == af.params().size()) {
                unify(existing, actual, b);
                return;
            }
            if (!compatibleRebind(existing, actual)) {
                // A RIGID/contravariant binding whose DECLARED type is an
                // abstract value head accepts actuals UP pure's lattice —
                // the variable KEEPS the declared type: eval over
                // {a:Number[1]|...} with a Float argument is the spec
                // (essential math testNumberExp/Log/Pow family, m3
                // hierarchy: Integer/Float/Decimal <: Number,
                // StrictDate/DateTime <: Date).
                if ((b.isRigid(v.name()) || b.contravariant())
                        && conformsUpValueLattice(actual, existing)) {
                    return;
                }
                // Real pure covariance closes over VALUE kinds too: two
                // incompatible value types meet at their LUB — the numeric
                // lattice for numbers, Any otherwise (mixed collections
                // travel as the variant carrier). Relation schemas stay
                // LOUD: a relation's identity is its columns.
                if (isValueKind(existing) && isValueKind(actual)
                        && !b.isRigid(v.name()) && !b.contravariant()) {
                    b.bindType(v.name(), valueLub(existing, actual));
                    return;
                }
                Type.RelationType exSchema = Type.schemaView(existing);
                Type.RelationType acSchema = Type.schemaView(actual);
                if (exSchema != null && acSchema != null) {
                    throw new TypeInferenceException("column mismatch: type variable "
                            + v.name() + " bound to relation "
                            + exSchema.columns().stream().map(Type.Column::name).toList()
                            + " cannot also bind relation "
                            + acSchema.columns().stream().map(Type.Column::name).toList());
                }
                throw new TypeInferenceException(
                        "type variable " + v.name() + " bound to " + existing.typeName()
                                + " cannot also bind " + actual.typeName());
            }
        } else {
            b.bindType(v.name(), actual);   // bind the actual unchanged
            if (b.contravariant()) {
                b.markRigid(v.name());
            }
        }
    }

    /** A concrete VALUE kind — the types real-pure covariance LUBs to Any. */
    private static boolean isValueKind(Type t) {
        return t instanceof Type.Primitive || t instanceof Type.PrecisionDecimal
                || t instanceof Type.EnumType || t instanceof Type.ClassType;
    }

    private static Type valueLub(Type a, Type b2) {
        if (isNumeric(a) && isNumeric(b2)) {
            return Type.Primitive.NUMBER;
        }
        return new Type.ClassType(ANY_FQN);
    }

    /** {@code actual} conforms to a DECLARED abstract value head via
     * the m3 primitive hierarchy (Number and Date are the abstract
     * heads; everything else is exact — {@code compatibleRebind}'s
     * business). */
    private static boolean conformsUpValueLattice(Type actual, Type declared) {
        if (declared == Type.Primitive.NUMBER) {
            return isNumeric(actual);
        }
        if (declared == Type.Primitive.DATE) {
            return actual == Type.Primitive.STRICT_DATE
                    || actual == Type.Primitive.DATE_TIME;
        }
        return false;
    }

    private static boolean isNumeric(Type t) {
        return t instanceof Type.PrecisionDecimal
                || (t instanceof Type.Primitive p
                        && (p == Type.Primitive.NUMBER || p == Type.Primitive.INTEGER
                                || p == Type.Primitive.FLOAT || p == Type.Primitive.DECIMAL));
    }

    private static boolean isNil(Type t) {
        return t instanceof Type.ClassType c
                && c.fqn().equals(NIL_FQN);
    }

    /**
     * Whether {@code t} still contains a type variable unsolved in {@code b}
     * ({@code ?} excluded — it is not a solvable variable). Function types are
     * leaves, mirroring {@link #resolve}.
     */
    public boolean hasFreeTypeVars(Type t, Bindings b) {
        return switch (t) {
            case Type.TypeVar v -> !isUnknown(v) && !b.hasType(v.name());
            case Type.GenericType g -> g.arguments().stream().anyMatch(a -> hasFreeTypeVars(a, b));
            case Type.RelationType r -> r.columns().stream().anyMatch(c -> hasFreeTypeVars(c.type(), b));
            case Type.SchemaAlgebra sa -> hasFreeTypeVars(sa.left(), b) || hasFreeTypeVars(sa.right(), b);
            case Type.FunctionType ignored -> false;
            case Type.Primitive ignored -> false;
            case Type.PrecisionDecimal ignored -> false;
            case Type.ClassType ignored -> false;
            case Type.EnumType ignored -> false;
        };
    }

    /** A row-struct that is still entirely unsolved (every column type is {@code ?}). */
    private static boolean isUnknownFragment(Type.RelationType r) {
        return !r.columns().isEmpty() && r.columns().stream().allMatch(c -> isUnknown(c.type()));
    }

    private void requirePrimitiveSubtype(Type actual, Type formal) {
        // Nil is BOTTOM: the []-born value conforms to every primitive slot
        // (corr(x, []) / splitPart([], ...) are the empty-in, empty-out PCTs).
        if (isNil(actual)) {
            return;
        }
        if (!isPrimitiveSubtype(actual, formal)) {
            throw fail(formal, actual);
        }
    }

    /** Per-column unification of a concrete row-struct against a formal one (match by name). */
    private void unifyColumns(Type.RelationType formal, Type.RelationType actual, Bindings b) {
        for (Type.Column fc : formal.columns()) {
            Type.Column ac = actual.columns().stream()
                    .filter(c -> sameColumn(c.name(), fc.name()))
                    .findFirst()
                    .orElseThrow(() -> new TypeInferenceException(
                            "relation is missing expected column '" + fc.name() + "'"));
            try {
                unify(fc.type(), ac.type(), b);
            } catch (TypeInferenceException e) {
                // Re-raise with COLUMN CONTEXT (the resolveChosen call-boundary
                // pattern): a bare "expected String, got Integer" over a wide
                // schema names no column at all.
                throw new TypeInferenceException(
                        "column '" + fc.name() + "': " + e.getMessage(), e);
            }
        }
    }

    /**
     * Unify an actual argument's multiplicity against a parameter's: binds a
     * multiplicity variable, otherwise validates FULL covariant containment
     * &mdash; real pure's {@code MultiplicityMatch} (legend-pure
     * {@code m3/navigation/multiplicity/MultiplicityMatch.java:273-279}):
     * {@code [a..b]} conforms to {@code [c..d]} iff {@code c <= a} and
     * {@code b <= d}. In particular {@code [0..1]} does NOT conform to
     * {@code [1]} &mdash; that is precisely why {@code toOne()} exists.
     * (The earlier claim here that "engine convention" rejects only
     * {@code [*] -> [1]} was FALSE &mdash; multiplicity audit
     * docs/MULTIPLICITY_AUDIT_2026_08_20.md §1: it manufactured a false
     * {@code [1]} on the most common expression shape in Legend.)
     * Validation is <strong>skipped for relation sources</strong> (§3.2
     * &mdash; relation ops are typed {@code [*]} but their signatures say
     * {@code [1]}).
     */
    public void unifyMult(Multiplicity formal, Multiplicity actual, Type actualType, Bindings b) {
        unifyMult(formal, actual, actualType, b, false);
    }

    /**
     * {@code contravariantSlot} is true ONLY for a function value's own
     * PARAMETER slots (a wider actual accepts there — real pure); the frame
     * flag was too broad: a nested function type's RESULT unified inside an
     * active frame and wrongly accepted a many-valued result (audit).
     */
    public void unifyMult(Multiplicity formal, Multiplicity actual, Type actualType, Bindings b,
                          boolean contravariantSlot) {
        switch (formal) {
            case Multiplicity.Var v -> {
                if (!b.hasMult(v.name())) {
                    b.bindMult(v.name(), actual);
                } else if (b.mult(v.name()).orElseThrow() instanceof Multiplicity.Bounded e
                        && actual instanceof Multiplicity.Bounded a2
                        && !contains(e, a2)) {
                    // COVARIANT accumulation: a shared multiplicity variable
                    // widens to the RANGE UNION of its occurrences — fold's
                    // []-init [0] meets a [*] body at [*] (real pure's own
                    // PCT folds carry [2]-annotated accumulators with [1..3]
                    // bodies). Reachable via the shared-mult-var natives
                    // (fold/eval); if() computes its multiplicity in
                    // IfChecker and never routes here. Widening only (a
                    // contained range keeps the solution stable). ONE
                    // owner: Multiplicity.union (audit §1d).
                    b.bindMult(v.name(), Multiplicity.union(e, a2));
                }
            }
            case Multiplicity.Bounded fb -> {
                // FULL covariant containment (MultiplicityMatch): both
                // bounds. Carve-outs, each with its own doctrine:
                // relation sources skip (§3.2); CONTRAVARIANT position (a
                // function value's parameters) accepts a wider actual —
                // equal(Any[*],Any[*]) is a legal {T[1],T[1]->Boolean}
                // comparator in real pure; a Variant MANY conforms to a
                // to-one slot — a collection of variants IS one JSON
                // array value (toMany(@Variant) results flow into to-one
                // column/argument slots as array cells).
                boolean relationSource = Type.isRelation(actualType);
                if (!relationSource && !contravariantSlot
                        && actual instanceof Multiplicity.Bounded ab) {
                    boolean variantCarrier = com.legend.compiler.element
                            .type.PlatformTypes.isVariant(actualType);
                    boolean upperOk = fb.upper() == null
                            || (ab.upper() != null && ab.upper() <= fb.upper())
                            || variantCarrier;
                    // the LOWER bound: [0..1] into [1] is a REAL pure
                    // error (that is why toOne() exists). Nil-as-bottom
                    // conforms on the TYPE lattice, but multiplicity
                    // still binds (abs([]) and f():Integer[1]{[]} are
                    // real-pure errors).
                    boolean lowerOk = fb.lower() <= ab.lower();
                    if (!upperOk || !lowerOk) {
                        throw new TypeInferenceException("multiplicity "
                                + ab.text() + " is not compatible with "
                                + fb.text());
                    }
                }
            }
        }
    }

    // =====================================================================
    // Resolution &mdash; substitute solved variables back into a type
    // =====================================================================

    /**
     * Substitute the solved variables in {@code b} into {@code t}. Unwraps a
     * {@code Relation<row>} to its bare row-struct (the value form, G-&alpha;) and
     * evaluates {@link Type.SchemaAlgebra}. Throws on an unbound variable.
     */
    /** Variables whose bindings are being resolved right now (the cycle guard). */
    private final java.util.Set<String> resolving = new java.util.HashSet<>();

    /** The ENCLOSING FUNCTION's declared type/multiplicity parameters, one
     * frame per body being typed (Typer.inFunctionScope): inside the body
     * they are RIGID — a call whose bindings never bind them resolves them
     * to themselves ($comparator->eval($value, $x) in contains<Z>). */
    private final java.util.ArrayDeque<java.util.Set<String>> rigidFrames =
            new java.util.ArrayDeque<>();

    /** Run {@code body} with {@code names} rigid. */
    public <R> R withRigid(java.util.Collection<String> names, java.util.function.Supplier<R> body) {
        rigidFrames.push(new java.util.HashSet<>(names));
        try {
            return body.get();
        } finally {
            rigidFrames.pop();
        }
    }

    private boolean isRigid(String name) {
        return !rigidFrames.isEmpty() && rigidFrames.peek().contains(name);
    }

    public Type resolve(Type t, Bindings b) {
        return switch (t) {
            // The unknown column type `?` of a colspec VALUE is not a solvable variable —
            // it passes through untouched (⊆/= replace it before it can reach an output schema).
            case Type.TypeVar v when isUnknown(v) -> t;
            // a variable bound to a type that itself carries variables (V := Z,
            // Z := Integer — the enclosing function's parameters bound per call)
            // resolves THROUGH the chain; a self-binding stops it
            case Type.TypeVar v -> {
                if (b.type(v.name()).isEmpty() && isRigid(v.name())) {
                    yield t;    // the enclosing function's own parameter, unbound here: itself
                }
                Type bound = b.type(v.name()).orElseThrow(() ->
                        new TypeInferenceException("unbound type variable " + v.name()));
                // CYCLE GUARD across the whole resolution (T := G<W>, W := G<T>):
                // a variable met again while its own binding is being resolved
                // stays as-is — no finite resolution exists
                if (!resolving.add(v.name())) {
                    yield t;
                }
                try {
                    yield bound instanceof Type.TypeVar tv && tv.name().equals(v.name())
                            ? bound : resolve(bound, b);
                } finally {
                    resolving.remove(v.name());
                }
            }

            // Relation<T> stays WRAPPED (the G-α erasure is deleted): T
            // resolves to the bound schema struct and the container
            // rides through the generic descent below — a table type
            // leaves resolution as GenericType(Relation, [schema]).
            case Type.GenericType g -> new Type.GenericType(g.rawFqn(),
                    g.arguments().stream().map(a -> resolve(a, b)).toList(),
                    // Result<T|m>-style multiplicity arguments resolve
                    // through the SAME bindings the if/let machinery
                    // fills (leg 2 — dropping them erased Result's m)
                    g.multArguments().stream()
                            .map(m -> resolveMultIfBound(m, b)).toList());

            case Type.SchemaAlgebra sa -> resolveSchemaAlgebra(sa, b);
            // dynamicColumns (pivot templates) RIDE resolution — rebuilding
            // without them silently degraded downstream pivot column typing
            // to SQL-type derivation (audit finding).
            case Type.RelationType r -> new Type.RelationType(
                    resolveColumns(r.columns(), b), r.dynamicColumns());

            // A function-typed OUTPUT carries solved variables inside its
            // params/result (preval's Function<{->T[*]}> — the identity
            // wrapper's T must leave as the query's concrete type, or the
            // raw variable escapes into execute's Result<T>). Unbound
            // inner variables ride through untouched (the pre-descent
            // leaf behavior, kept for higher-order shapes solved later).
            case Type.FunctionType f -> {
                List<Type.Param> ps = new ArrayList<>(f.params().size());
                for (Type.Param p : f.params()) {
                    ps.add(new Type.Param(resolveIfSolvable(p.type(), b),
                            resolveMultIfBound(p.multiplicity(), b)));
                }
                yield new Type.FunctionType(ps, new Type.Param(
                        resolveIfSolvable(f.result().type(), b),
                        resolveMultIfBound(f.result().multiplicity(), b)));
            }

            // Leaves: no variables to substitute.
            case Type.Primitive ignored -> t;
            case Type.PrecisionDecimal ignored -> t;
            case Type.ClassType ignored -> t;
            case Type.EnumType ignored -> t;
        };
    }

    /** {@link #resolve} that leaves an UNBOUND variable in place instead of
     * throwing — inside a function type an open variable is legal (it binds
     * at the eventual application site). */
    private Type resolveIfSolvable(Type t, Bindings b) {
        if (t instanceof Type.TypeVar v && !isUnknown(v)
                && b.type(v.name()).isEmpty()) {
            return t;
        }
        return resolve(t, b);
    }

    private Multiplicity resolveMultIfBound(Multiplicity m, Bindings b) {
        return m instanceof Multiplicity.Var v
                ? b.mult(v.name()).orElse(m) : m;
    }

    private List<Type.Column> resolveColumns(List<Type.Column> columns, Bindings b) {
        List<Type.Column> out = new ArrayList<>(columns.size());
        for (Type.Column c : columns) {
            out.add(new Type.Column(c.name(), resolve(c.type(), b), c.multiplicity()));
        }
        return out;
    }

    /** {@code T+V} (union of schemas) and {@code T-Z} (drop named columns). */
    private Type resolveSchemaAlgebra(Type.SchemaAlgebra sa, Bindings b) {
        Type left = resolve(sa.left(), b);
        if (!(left instanceof Type.RelationType lr)) {
            throw new TypeInferenceException(
                    "schema-algebra left operand is not a relation: " + left.typeName());
        }
        Type right = resolve(sa.right(), b);
        switch (sa.op()) {
            case UNION -> {
                List<Type.Column> cols = new ArrayList<>(lr.columns());
                if (right instanceof Type.RelationType rr) {
                    for (Type.Column c : rr.columns()) {
                        // Real legend-pure errors on a name collision (extend/rename/join/
                        // groupBy adding a column that already exists) — never silent.
                        if (lr.columns().stream().anyMatch(e -> sameColumn(e.name(), c.name()))) {
                            throw new SchemaInvariantException("the column '" + c.name()
                                    + "' already exists in the relation " + lr.typeName());
                        }
                        cols.add(c);
                    }
                }
                // The LEFT operand's pivot templates ride through schema
                // UNION (extend over a pivot keeps its dynamic columns).
                return new Type.RelationType(cols, lr.dynamicColumns());
            }
            case DIFFERENCE -> {
                Set<String> drop = new LinkedHashSet<>();
                if (right instanceof Type.RelationType rr) {
                    rr.columns().forEach(c -> drop.add(c.name()));
                }
                List<Type.Column> cols = new ArrayList<>();
                for (Type.Column c : lr.columns()) {
                    if (!drop.contains(c.name())) {
                        cols.add(c);
                    }
                }
                return new Type.RelationType(cols, lr.dynamicColumns());
            }
            default -> throw new TypeInferenceException(
                    "schema-algebra operator not supported in resolution: " + sa.op());
        }
    }

    /** Whether range {@code outer} already contains range {@code inner}. */
    private static boolean contains(Multiplicity.Bounded outer, Multiplicity.Bounded inner) {
        boolean upperOk = outer.upper() == null
                || (inner.upper() != null && inner.upper() <= outer.upper());
        return outer.lower() <= inner.lower() && upperOk;
    }

    /** Resolve a return multiplicity: a {@link Multiplicity.Var} is looked up, otherwise identity. */
    public Multiplicity resolveMult(Multiplicity m, Bindings b) {
        return switch (m) {
            case Multiplicity.Var v -> {
                Multiplicity bound = b.mult(v.name()).orElseThrow(() ->
                        new TypeInferenceException("unbound multiplicity variable " + v.name()));
                java.util.Set<String> seen = new java.util.HashSet<>(java.util.List.of(v.name()));
                while (bound instanceof Multiplicity.Var bv && seen.add(bv.name())
                        && b.mult(bv.name()).isPresent()) {
                    bound = b.mult(bv.name()).orElseThrow();
                }
                yield bound;
            }
            case Multiplicity.Bounded ignored -> m;
        };
    }

    /** The resolved {@code (type, multiplicity)} a call produces (a top-level {@link ExprType}). */
    public ExprType resolveOutput(Type returnType, Multiplicity returnMult, Bindings b) {
        return new ExprType(resolve(returnType, b), resolveMult(returnMult, b));
    }

    /** The resolved output of a CALL — {@link #resolveOutput(Type, Multiplicity,
     *  Bindings)} plus the result half of TDS ERASURE ({@link TdsErasure}): a
     *  declared TabularDataSet output IS the argument's actual relation. Every
     *  path that types a call's value (eager and deferred) reads this one. */
    public ExprType resolveOutput(Type returnType, Multiplicity returnMult, Bindings b,
            List<ExprType> args) {
        return TdsErasure.refineResult(ctx, args, resolveOutput(returnType, returnMult, b));
    }

    // =====================================================================
    // Overload resolution (engine AbstractChecker:82-226; §3.1)
    // =====================================================================

    /**
     * Pick the single best-matching overload from {@code candidates} for the
     * given concrete argument types, then resolve the call's output type.
     * Arity filter &rarr; specificity scoring (type exact=2/subtype=1/var=0 &times;10
     * + multiplicity exact=5/[1]=4/[0..1]=3/[1..*]=2/[*]=1/var=0) &rarr; highest
     * wins, a tie throws (G-&beta;).
     *
     * <p>Scope: scalar / native resolution over already-typed arguments. Lambda
     * arguments (function-typed params) and class-subtype user-argument checking
     * arrive with the bidirectional body checker.
     */
    public Resolution resolveOverload(List<TypedFunction> candidates, List<ExprType> args) {
        return resolveOverload(candidates, args, null);
    }

    /** {@link #resolveOverload(List, List)} with the CALLER'S EXPECTED type of the
     *  call's value, when the caller knows it — real pure's bidirectional
     *  inference: the expected type unifies with the candidate's declared return
     *  type BEFORE its parameters, so a type parameter no parameter binds
     *  (over<T>(cols:ColSpec<(?:?)⊆T>):_Window<T>, T = the enclosing extend's
     *  relation schema) is bound from the context, never guessed. */
    public Resolution resolveOverload(List<TypedFunction> candidates, List<ExprType> args,
            @com.legend.base.Nullable Type expected) {
        // Diagnostics carry the FUNCTION NAME (from the candidates — every
        // caller has homogeneous candidates); "no overload accepts 2
        // argument(s)" with no callee was an audit finding.
        String name = candidates.isEmpty() ? "?" : candidates.get(0).qualifiedName();
        List<TypedFunction> arityMatches = new ArrayList<>();
        for (TypedFunction c : candidates) {
            if (c.parameters().size() == args.size()) {
                arityMatches.add(c);
            }
        }
        if (arityMatches.isEmpty()) {
            throw new TypeInferenceException("no overload of '" + name + "' accepts "
                    + args.size() + " argument(s)");
        }
        if (arityMatches.size() == 1) {
            return resolveChosen(arityMatches.get(0), args, name, expected);
        }

        long best = Long.MIN_VALUE;
        List<TypedFunction> winners = new ArrayList<>();
        for (TypedFunction c : arityMatches) {
            long s = score(c, args);
            if (s < 0) {
                continue;   // structural non-match
            }
            if (s > best) {
                best = s;
                winners.clear();
                winners.add(c);
            } else if (s == best) {
                winners.add(c);
            }
        }
        if (winners.isEmpty()) {
            StringBuilder detail = new StringBuilder();
            for (ExprType a : args) {
                detail.append(detail.length() == 0 ? "" : ", ").append(a);
            }
            StringBuilder cands = new StringBuilder();
            for (TypedFunction c : arityMatches) {
                cands.append("; ").append(c.parameters());
            }
            throw new TypeInferenceException("no overload of '" + name
                    + "' structurally matches the argument types ("
                    + detail + ")" + cands);
        }
        if (winners.size() > 1) {
            // DUPLICATE-SIGNATURE tolerance: distinct FQNs registering the
            // SAME parameter signature (mapping::execute vs
            // router::execute — one execution semantics, two real-pure
            // entry spellings) are interchangeable at the type level; the
            // first wins deterministically. Genuinely different signatures
            // still throw.
            boolean allSameShape = winners.stream().allMatch(w ->
                    w.parameters().equals(winners.get(0).parameters())
                            && w.returnType().equals(
                                    winners.get(0).returnType())
                            && w.returnMultiplicity().equals(
                                    winners.get(0).returnMultiplicity()));
            if (!allSameShape) {
                // NATIVE-over-module tie-break: a registered native carries
                // the PLATFORM's semantics for the name (the emission-level
                // implementation); a module copy tying with it is the same
                // real-pure function whose reflection body this platform
                // cannot run (concatenateTemporalTdsQueries). A tie among
                // module definitions alone stays loud.
                List<TypedFunction> nativeWinners = winners.stream()
                        .filter(TypedFunction::isNative).toList();
                // MOST-SPECIFIC rule (real pure: resolvePrimaryKey(Root) /
                // (RelationalInstanceSetImplementation) / (InstanceSet
                // Implementation) all match a Root argument and the engine
                // binds the Root one): a module candidate whose class-typed
                // parameters are all subtypes of every other candidate's
                // wins the tie among module definitions.
                if (nativeWinners.isEmpty()) {
                    TypedFunction specific = mostSpecific(winners);
                    if (specific != null) {
                        return resolveChosen(specific, args, name, expected);
                    }
                }
                if (nativeWinners.size() != 1) {
                    // ENGINE bottom-value rule (GenericTypeMatch
                    // MATCH_CAUTIOUSLY): a []-born argument is real pure's
                    // UNRESOLVED T at match time, and a non-concrete value
                    // matches ONLY a top-type or type-parameter formal —
                    // relation::toString(Relation<T>) never even matches
                    // the empty lambda's x where toString(Any) does
                    // (testRemoveDuplicatesEmptyListExplicit). Our kernel
                    // binds T=Nil eagerly, so the same rule applies here
                    // as a TIE-break: a Nil-typed argument narrows the
                    // tied winners to what real pure would have matched.
                    List<TypedFunction> byBottom = winners;
                    for (int i = 0; i < args.size(); i++) {
                        if (!PlatformTypes.isNil(args.get(i).type())) {
                            continue;
                        }
                        final int ai = i;
                        List<TypedFunction> kept = byBottom.stream()
                                .filter(w -> {
                                    Type p = w.parameters().get(ai).type();
                                    return PlatformTypes.isAny(p)
                                            || p instanceof Type.TypeVar;
                                }).toList();
                        if (!kept.isEmpty()) {
                            byBottom = kept;
                        }
                    }
                    if (byBottom.size() == 1) {
                        return resolveChosen(byBottom.get(0), args, name, expected);
                    }
                    // LINEARIZATION tie-break (real pure resolves a class's
                    // generalizations in declaration order): among UNRELATED
                    // class formals that all accept the argument, the one
                    // nearest in the argument's supertype order wins —
                    // elementToPath(Type) over (PackageableElement) for a
                    // Class value (m3: Class extends Type, …, PackageableElement)
                    TypedFunction nearest = nearestInLinearization(byBottom, args);
                    if (nearest != null) {
                        return resolveChosen(nearest, args, name, expected);
                    }
                    throw new TypeInferenceException("ambiguous overload of '" + name + "': "
                            + winners.size() + " candidates tie for the argument types ["
                            + winners.stream().map(w -> w.qualifiedName()
                                    + "/" + w.parameters().size()
                                    + (w.isNative() ? ":native" : ":module")
                                    + " p0=" + w.parameters().get(0).type().typeName()
                                    + " ret=" + w.returnType().typeName()
                                    + "[" + w.returnMultiplicity() + "]")
                                    .collect(java.util.stream.Collectors.joining("; "))
                            + "]");
                }
                return resolveChosen(nativeWinners.get(0), args, name, expected);
            }
        }
        return resolveChosen(winners.get(0), args, name, expected);
    }

    /**
     * Specificity score of {@code candidate} against {@code args}, counting only the
     * <strong>present</strong> positions (a {@code null} entry is a not-yet-typed
     * slot, e.g. a lambda argument, and is skipped); {@code -1} if any present
     * parameter does not match. Same scoring as {@link #resolveOverload}, so it is
     * the basis for selecting an overload from a call's non-lambda arguments &mdash;
     * crucially, it lets a relation source pick {@code Relation<T>} (a relation match,
     * type-score 1) over a generic {@code T[*]} (a type-var, score 0).
     */
    public long scoreNonLambda(TypedFunction candidate, List<ExprType> args) {
        if (candidate.parameters().size() != args.size()) {
            return -1;
        }
        long total = 0;
        for (int i = 0; i < args.size(); i++) {
            ExprType a = args.get(i);
            if (a == null) {
                continue;
            }
            TypedParameter p = candidate.parameters().get(i);
            int typeScore = paramTypeScore(p.type(), a.type());
            int multScore = paramMultScore(p.multiplicity(), a.multiplicity(), a.type());
            if (typeScore < 0 || multScore < 0) {
                return -1;
            }
            total += typeScore * 20L + multScore;
        }
        return total;
    }

    /** Specificity score of a candidate, or {@code -1} if any parameter does not match. */
    private long score(TypedFunction c, List<ExprType> args) {
        long total = 0;
        for (int i = 0; i < args.size(); i++) {
            TypedParameter p = c.parameters().get(i);
            int typeScore = paramTypeScore(p.type(), args.get(i).type());
            if (typeScore < 0) {
                return -1;
            }
            int multScore = paramMultScore(p.multiplicity(), args.get(i).multiplicity(), args.get(i).type());
            if (multScore < 0) {
                return -1;
            }
            total += typeScore * 20L + multScore;
        }
        return total;
    }

    /** Unify the chosen overload's parameters against the args, then resolve its output. */
    private Resolution resolveChosen(TypedFunction c, List<ExprType> args, String name,
            @com.legend.base.Nullable Type expected) {
        // the callee's own parameters RENAMED APART from the variables the
        // arguments carry (the enclosing function's own T, m): bindings are
        // keyed by name, and a coincident name is still another variable
        TypedFunction chosen = c;
        c = SignatureApart.of(c, args, expected);
        Bindings b = new Bindings();
        if (expected != null) {
            // the caller's expected type binds the declared return type's variables
            // first (a mismatch is the call's error, named)
            try {
                unify(c.returnType(), expected, b);
            } catch (TypeInferenceException e) {
                throw new TypeInferenceException("in call to '" + name + "': the expected "
                        + expected.typeName() + " does not fit its declared " + c.returnType().typeName()
                        + " — " + e.getMessage(), e);
            }
        }
        for (int i = 0; i < args.size(); i++) {
            TypedParameter p = c.parameters().get(i);
            try {
                unify(p.type(), args.get(i).type(), b);
                unifyMult(p.multiplicity(), args.get(i).multiplicity(), args.get(i).type(), b);
            } catch (TypeInferenceException e) {
                if (System.getenv("LL_TDG_DEBUG") != null) {
                    System.err.println("[tdg-debug] call '" + name + "' arg"
                            + (i + 1) + " exprType=" + args.get(i)
                            + " chosen=" + c.parameters());
                }
                // Re-raise with CALL CONTEXT — a bare "expected X, got Y"
                // reached corpus users with zero callee info (audit finding).
                throw new TypeInferenceException("in call to '" + name + "', argument "
                        + (i + 1) + ": " + e.getMessage(), e);
            }
        }
        return new Resolution(chosen, resolveOutput(c.returnType(), c.returnMultiplicity(), b, args));
    }

    /** The chosen overload and the {@link ExprType} the call produces. */
    public record Resolution(TypedFunction chosen, ExprType output) {
    }

    /**
     * Whether a concrete {@code actual} conforms to {@code formal} on the nominal
     * lattice (exact, primitive subtype, class subclass, Any) &mdash; the boolean
     * form of the overload-scoring test, for structural acceptance checks like
     * {@code match} branch dispatch.
     */
    public boolean accepts(Type formal, Type actual) {
        return paramTypeScore(formal, actual) >= 0;
    }

    // ---- scoring helpers ------------------------------------------------

    /** Type specificity: exact=2, subtype=1, type-var/Any=0, no match=-1. */
    private int paramTypeScore(Type formal, Type actual) {
        // Function<{...}> vs bare FunctionType: normalize PAIRWISE the way
        // unify() does — the two kernel halves must agree, or scoring
        // rejects what unification accepts (map-built lambda collections
        // against a Function<...>[*] param). A formal that keeps its
        // carrier nominal (FunctionDefinition<Any>) sees the actual's
        // carrier too — the generic arm's lattice rule judges it.
        Type nf = unwrapFunction(formal);
        boolean formalKeepsCarrier = nf == formal
                && ((formal instanceof Type.GenericType fg
                        && FUNCTION_CARRIER_FQNS.contains(fg.rawFqn()))
                    || PlatformTypes.isAny(formal));   // as in unify(): Any keeps the carrier
        // NOMINAL GATE mirroring unify() (audit R5): carrier-vs-carrier
        // is judged on the class lattice BEFORE structural unwrap.
        if (formal instanceof Type.GenericType nf2
                && FUNCTION_CARRIER_FQNS.contains(nf2.rawFqn())
                && actual instanceof Type.GenericType na2
                && FUNCTION_CARRIER_FQNS.contains(na2.rawFqn())
                && !na2.rawFqn().equals(nf2.rawFqn())
                && !ctx.isSubtype(na2.rawFqn(), nf2.rawFqn())) {
            return -1;
        }
        Type na = formalKeepsCarrier ? actual : unwrapFunctionValue(actual, nf);
        if (nf != formal || na != actual) {
            return paramTypeScore(nf, na);
        }
        // Nil is BOTTOM (the []-born value): it conforms to EVERY formal —
        // scoring must agree with unify's Nil arm, or a multi-overload
        // call rejects the [] argument the single-candidate path accepts
        // (createDbConfig($dbType, []) against corpus + prelude overloads).
        if (isNil(actual)) {
            return 0;
        }
        return switch (formal) {
            case Type.ClassType c when c.fqn().equals(ANY_FQN) -> 0;
            case Type.TypeVar ignored -> 0;

            case Type.Primitive ignored -> primitiveTypeScore(actual, formal);
            case Type.PrecisionDecimal ignored -> primitiveTypeScore(actual, formal);

            case Type.ClassType fc
                    when fc.fqn().equals(PlatformTypes.TABULAR_DATA_SET)
                    && Type.isRelation(actual) -> 1;
            case Type.ClassType fc -> {
                // a parameterized actual (Class<X>, Enumeration<E>) is
                // nominally its raw class — the unify arm's rule
                if (actual instanceof Type.GenericType ag) {
                    yield ag.rawFqn().equals(fc.fqn()) ? 2
                            : (ctx.isSubtype(ag.rawFqn(), fc.fqn()) ? 1 : -1);
                }
                if (!(actual instanceof Type.ClassType ac)) {
                    yield -1;
                }
                yield ac.fqn().equals(fc.fqn()) ? 2 : (ctx.isSubtype(ac.fqn(), fc.fqn()) ? 1 : -1);
            }
            case Type.EnumType fe ->
                    (actual instanceof Type.EnumType ae && ae.fqn().equals(fe.fqn())) ? 2 : -1;

            case Type.GenericType g when g.rawFqn().equals(RELATION_FQN) ->
                    Type.isRelation(actual) ? 1 : -1;
            // TDS = schema-erasing relation nominal (must agree with the
            // unify arm; score as a subtype-grade match).
            case Type.GenericType g
                    when g.rawFqn().equals(PlatformTypes.TABULAR_DATA_SET)
                    && Type.isRelation(actual) -> 1;
            case Type.GenericType g -> {
                // Nominal raw-class lattice, mirroring the unify arm: a
                // subclass raw scores like the exact raw (m3 function
                // carriers — LambdaFunction into FunctionDefinition<Any>).
                if (!(actual instanceof Type.GenericType ag)) {
                    yield -1;
                }
                if (!ag.rawFqn().equals(g.rawFqn())) {
                    yield ctx.isSubtype(ag.rawFqn(), g.rawFqn()) ? 1 : -1;
                }
                // Function<{...->V[m]}> carriers: a KNOWN function value
                // must fit the formal's interior RESULT multiplicity —
                // map's {T[1]->V[0..1]} overload cannot take a
                // {Firm[1]->Integer[*]} argument; real pure selects the
                // {T[1]->V[*]} overload (map.pure:28). Without this check
                // the tighter-VALUE overload won the score and died at
                // unification (testUsingFunctionInMapLambdaTakingAParameter).
                if (g.arguments().size() == 1
                        && g.arguments().get(0) instanceof Type.FunctionType ff
                        && ag.arguments().size() == 1
                        && ag.arguments().get(0) instanceof Type.FunctionType af) {
                    if (af.params().size() != ff.params().size()) {
                        yield -1;
                    }
                    if (ff.result().multiplicity() instanceof Multiplicity.Bounded fb
                            && resultMultScore(fb, af.result().multiplicity(),
                                    af.result().type()) < 0) {
                        yield -1;
                    }
                }
                yield 1;
            }

            case Type.RelationType ignored -> Type.schemaView(actual) != null ? 1 : -1;
            case Type.FunctionType ff -> {
                if (!(actual instanceof Type.FunctionType af)) {
                    yield -1;
                }
                if (af.params().size() != ff.params().size()) {
                    yield -1;
                }
                // A KNOWN function value must fit the formal's RESULT
                // multiplicity: map's {T[1]->V[0..1]} overload cannot take a
                // {Firm[1]->Integer[*]} argument — real pure selects the
                // {T[1]->V[*]} overload (map.pure:28). Without this interior
                // check the tighter-VALUE overload won the score and died at
                // unification (testUsingFunctionInMapLambdaTakingAParameter).
                if (ff.result().multiplicity() instanceof Multiplicity.Bounded fb
                        && resultMultScore(fb, af.result().multiplicity(),
                                af.result().type()) < 0) {
                    yield -1;
                }
                yield 1;
            }

            // Schema algebra never appears as a parameter type to score against.
            case Type.SchemaAlgebra ignored -> -1;
        };
    }

    private int primitiveTypeScore(Type actual, Type formal) {
        String f = primitiveFqn(formal), a = primitiveFqn(actual);
        if (a == null || f == null) {
            return -1;
        }
        return a.equals(f) ? 2 : (ctx.isSubtype(a, f) ? 1 : -1);
    }

    /** Multiplicity specificity: exact=10, mult-VAR=9 (a var BINDS the
     * argument's multiplicity exactly, so it beats every SUBSUMING
     * concrete overload and loses only to an exact concrete match — real
     * pure picks map's {T[m]->V[m]} over {T[0..1]->V[0..1]} for a [1]
     * source), then tightness 8/6/4/2; {@code -1} rejects {@code [*]->[1]}. */
    /** Result-slot scoring twin of {@link #unifyMultResult}: upper bound
     * only — scoring and unification must agree (the kernel-halves rule),
     * and lambda results widen [0..1] into [1] slots per the reference's
     * observed covariance. */
    private int resultMultScore(Multiplicity.Bounded formal, Multiplicity actual,
            Type actualType) {
        if (actual instanceof Multiplicity.Bounded ab
                && !Type.isRelation(actualType)
                && !com.legend.compiler.element.type.PlatformTypes
                        .isVariant(actualType)) {
            boolean upperOk = formal.upper() == null
                    || (ab.upper() != null && ab.upper() <= formal.upper());
            if (!upperOk) {
                return -1;
            }
        }
        return multiplicityTightness(formal);
    }

    private int paramMultScore(Multiplicity formal, Multiplicity actual, Type actualType) {
        return switch (formal) {
            case Multiplicity.Var ignored -> 9;
            case Multiplicity.Bounded fb -> {
                if (fb.equals(actual)) {
                    yield 10;
                }
                // FULL covariant containment, mirroring unifyMult (the
                // two kernel halves must agree, or selection picks an
                // overload the check then rejects). This is also HOW
                // real pure disambiguates [0..1]-vs-[1] overload pairs:
                // a [0..1] actual structurally cannot take the [1]
                // overload. Relation sources skip (§3.2); a Variant
                // MANY conforms to a to-one slot (the carrier rule).
                if (actual instanceof Multiplicity.Bounded ab
                        && !Type.isRelation(actualType)) {
                    boolean upperOk = fb.upper() == null
                            || (ab.upper() != null && ab.upper() <= fb.upper())
                            || com.legend.compiler.element.type.PlatformTypes
                                    .isVariant(actualType);
                    if (!upperOk || fb.lower() > ab.lower()) {
                        yield -1;
                    }
                }
                yield multiplicityTightness(fb);
            }
        };
    }

    private static int multiplicityTightness(Multiplicity.Bounded m) {
        Integer up = m.upper();
        int lo = m.lower();
        if (lo == 1 && up != null && up == 1) {
            return 8;   // [1]
        }
        if (lo == 0 && up != null && up == 1) {
            return 6;   // [0..1]
        }
        if (lo == 1 && up == null) {
            return 4;   // [1..*]
        }
        if (lo == 0 && up == null) {
            return 2;   // [*]
        }
        return 0;
    }

    // =====================================================================
    // The type lattice — join (least upper bound) over nominal types
    // =====================================================================

    /**
     * The least common supertype of two types &mdash; the JOIN the checker needs
     * where unification cannot go: {@code if} branches and collection elements
     * (join &ne; unify: this language has subtyping, so branches meet at their
     * least upper bound, not at equality).
     */
    /** The m3 function METACLASSES as a generic carrier — the nominal type
     * every lambda literal conforms to. */
    private static boolean isNominalFunctionCarrier(Type t) {
        if (!(t instanceof Type.GenericType g)) {
            return false;
        }
        return FUNCTION_CARRIER_FQNS.contains(g.rawFqn());
    }

    /** One side names the class RAW and the other parameterized, same class. */
    private static boolean rawOfSameClass(Type a, Type b) {
        return (a instanceof Type.ClassType c && b instanceof Type.GenericType g && g.rawFqn().equals(c.fqn()))
                || (b instanceof Type.ClassType c2 && a instanceof Type.GenericType g2 && g2.rawFqn().equals(c2.fqn()));
    }

    public Type commonSupertype(Type a, Type b) {
        if (a.equals(b)) {
            return a;
        }
        // raw vs parameterized, same class: the raw side is the class over Any
        if (rawOfSameClass(a, b)) {
            return a instanceof Type.GenericType ? a : b;
        }
        // Nil is the BOTTOM: the []-born branch joins to the other side
        // (if($x->isEmpty(), |[], |$enum->extractEnumValue(...)) : T[0..1])
        if (isNil(a)) {
            return b;
        }
        if (isNil(b)) {
            return a;
        }
        // Any is the TOP: its join with anything — a lambda's structural
        // type included — is Any (real pure findBestCommonGenericType;
        // the engine's function registry lists opaque and spelled
        // function values side by side, Phase 5 batch 147)
        if ((isAny(a) || isAny(b))) {
            return new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY);
        }
        // Same-raw schema-fragment containers (SortInfo<row>, ColSpec<row>, …): the LUB is the
        // container of the MERGED row — this is how [asc(~a), desc(~b)] becomes one
        // SortInfo<(a:?, b:?)> element type, whose ⊆T then accumulates every key.
        if (a instanceof Type.GenericType ga && b instanceof Type.GenericType gb
                && ga.rawFqn().equals(gb.rawFqn())
                && ga.arguments().size() == 1 && gb.arguments().size() == 1
                && ga.arguments().get(0) instanceof Type.RelationType ra
                && gb.arguments().get(0) instanceof Type.RelationType rb) {
            return new Type.GenericType(ga.rawFqn(), List.of(unionRows(ra, rb)));
        }
        // Bare relation pair: the LUB is the merged row (same rule as the
        // schema-fragment containers above).
        if (a instanceof Type.RelationType ra && b instanceof Type.RelationType rb) {
            return new Type.RelationType(unionRows(ra, rb).columns());
        }
        // Same-raw parameterized classes (Pair<String,String> vs
        // Pair<String,Integer>): the LUB is ARG-WISE — real covariance
        // (Pair<String, Any> holds both).
        if (a instanceof Type.GenericType gpa && b instanceof Type.GenericType gpb
                && gpa.rawFqn().equals(gpb.rawFqn())
                && gpa.arguments().size() == gpb.arguments().size()) {
            List<Type> lub = new java.util.ArrayList<>(gpa.arguments().size());
            for (int i = 0; i < gpa.arguments().size(); i++) {
                lub.add(commonSupertype(gpa.arguments().get(i), gpb.arguments().get(i)));
            }
            return new Type.GenericType(gpa.rawFqn(), lub);
        }
        // A lambda's STRUCTURAL type meets the NOMINAL function carrier
        // (Function<Any>, FunctionDefinition<…>, LambdaFunction<…>): every
        // lambda is a Function, so the LUB is the carrier (real pure:
        // LambdaFunction ≤ FunctionDefinition ≤ Function — the engine's
        // extension record concatenates such pairs; Phase 5 batch 147)
        if (a instanceof Type.FunctionType && isNominalFunctionCarrier(b)) {
            return b;
        }
        if (b instanceof Type.FunctionType && isNominalFunctionCarrier(a)) {
            return a;
        }
        // FunctionType LUB (engine GenericType.findBestCommonGenericType,
        // the isFunction arm — match over function values): different
        // arities join at Any; same arity builds the function type whose
        // params take the CONTRAVARIANT meet (commonSubtype below) and
        // whose return takes the ordinary covariant join, multiplicities
        // min-subsuming on both (Multiplicity.union IS the engine's
        // minSubsumingMultiplicity for two).
        if (a instanceof Type.FunctionType ffa && b instanceof Type.FunctionType ffb) {
            if (ffa.params().size() != ffb.params().size()) {
                return new Type.ClassType(
                        com.legend.compiler.element.type.PlatformTypes.ANY);
            }
            List<Type.Param> ps = new java.util.ArrayList<>(ffa.params().size());
            for (int i = 0; i < ffa.params().size(); i++) {
                Type.Param pa = ffa.params().get(i);
                Type.Param pb = ffb.params().get(i);
                ps.add(new Type.Param(commonSubtype(pa.type(), pb.type()),
                        Multiplicity.union(pa.multiplicity(), pb.multiplicity())));
            }
            return new Type.FunctionType(ps, new Type.Param(
                    commonSupertype(ffa.result().type(), ffb.result().type()),
                    Multiplicity.union(ffa.result().multiplicity(),
                            ffb.result().multiplicity())));
        }
        // The SCALAR-SUBQUERY ENCODING (a single-column relation in value
        // position — the fnlr discipline; the lowerer renders it as a
        // correlated scalar subquery) meeting a bare scalar in a branch or
        // collection LUB: the LUB is over the COLUMN's type — the
        // encoding's declared value semantics. Multi-column relations stay
        // at the loud non-nominal wall below. (Row-vs-Relation: the
        // encoding may arrive wrapped — a table — or as a bare
        // single-column row; both take the column's-type view.)
        Type.RelationType sv1 = Type.schemaView(a);
        Type.RelationType sv2 = Type.schemaView(b);
        if (sv1 != null && sv1.columns().size() == 1 && sv2 == null) {
            return commonSupertype(sv1.columns().get(0).type(), b);
        }
        if (sv2 != null && sv2.columns().size() == 1 && sv1 == null) {
            return commonSupertype(a, sv2.columns().get(0).type());
        }
        String fa = nominalFqn(a), fb = nominalFqn(b);
        if (fa == null || fb == null) {
            // NON-NOMINAL mismatch (function vs relation, differing function
            // shapes, …): LOUD — silently widening to Any hid branch-type
            // conflicts until they failed incomprehensibly downstream
            // (audit finding).
            throw new TypeInferenceException("no common supertype for "
                    + a.typeName() + " and " + b.typeName());
        }
        if (ctx.isSubtype(fa, fb)) {
            return b;
        }
        if (ctx.isSubtype(fb, fa)) {
            return a;
        }
        for (String ancestor : ancestorsOf(fa)) {   // nearest-first: the first shared supertype is the LCA
            if (ctx.isSubtype(fb, ancestor)) {
                return ctx.findType(ancestor).orElseGet(InferenceKernel::anyType);
            }
        }
        return anyType();
    }

    /**
     * The lattice's MEET — {@link #commonSupertype}'s dual, consumed by the
     * FunctionType LUB's contravariant parameter slots. Engine
     * {@code Support.getBestGenericTypeUsingContravariance}, the two-type
     * case: Nil absorbs, Any defers, a subtype wins, unrelated types meet
     * at Nil (never a loud wall — the engine returns bottom).
     */
    public Type commonSubtype(Type a, Type b) {
        if (a.equals(b)) {
            return a;
        }
        if (com.legend.compiler.element.type.PlatformTypes.isNil(a)
                || com.legend.compiler.element.type.PlatformTypes.isNil(b)) {
            return new Type.ClassType(
                    com.legend.compiler.element.type.PlatformTypes.NIL);
        }
        if (com.legend.compiler.element.type.PlatformTypes.isAny(a)) {
            return b;
        }
        if (com.legend.compiler.element.type.PlatformTypes.isAny(b)) {
            return a;
        }
        String fa = nominalFqn(a), fb = nominalFqn(b);
        if (fa != null && fb != null) {
            if (ctx.isSubtype(fa, fb)) {
                return a;
            }
            if (ctx.isSubtype(fb, fa)) {
                return b;
            }
        }
        return new Type.ClassType(
                com.legend.compiler.element.type.PlatformTypes.NIL);
    }

    /** {@code Function<{sig}>} unwraps to its bare {@code FunctionType}; everything else passes through. */
    /** The function-carrier nominals of real pure's m3 hierarchy —
     * LambdaFunction&lt;T&gt; extends FunctionDefinition&lt;T&gt; extends
     * Function&lt;T&gt;; each is a wrapper spelling of the bare
     * FunctionType it carries. */
    static final java.util.Set<String> FUNCTION_CARRIER_FQNS =
            com.legend.compiler.element.type.PlatformTypes.FUNCTION_CARRIERS;

    private static Type unwrapFunction(Type t) {
        if (t instanceof Type.GenericType g
                && FUNCTION_CARRIER_FQNS.contains(g.rawFqn())
                && g.arguments().size() == 1
                && g.arguments().get(0) instanceof Type.FunctionType inner) {
            return inner;
        }
        return t;
    }

    /** The ACTUAL side of a function-typed formal: a carrier unwraps as
     * above, and an m3 Function subclass value unwraps through its
     * instantiated supertype — only when the formal is structural, so a
     * plain T formal still binds the nominal Property. */
    private Type unwrapFunctionValue(Type actual, Type unwrappedFormal) {
        Type t = unwrapFunction(actual);
        if (t != actual || !(unwrappedFormal instanceof Type.FunctionType)) {
            return t;
        }
        return deepUnwrapFunction(actual);
    }

    private Type deepUnwrapFunction(Type t) {
        Type u = unwrapFunction(t);
        if (u != t) {
            return u;
        }
        // any OTHER Function subclass (m3: Property<U,V|m> ⊆ AbstractProperty
        // <{U[1]->V[m]}> ⊆ Function) unwraps through its instantiated
        // supertype — a property VALUE is a function value (eval, map)
        String raw = t instanceof Type.GenericType g2 ? g2.rawFqn()
                : t instanceof Type.ClassType c ? c.fqn() : null;
        if (raw != null && !FUNCTION_CARRIER_FQNS.contains(raw)
                && !raw.equals(com.legend.compiler.element.type.PlatformTypes.FUNCTION)
                && ctx.isSubtype(raw, com.legend.compiler.element.type.PlatformTypes.FUNCTION)) {
            Type asFn = asSuper(t, com.legend.compiler.element.type.PlatformTypes.FUNCTION)
                    .orElse(null);
            if (asFn instanceof Type.GenericType fg && fg.arguments().size() == 1
                    && fg.arguments().get(0) instanceof Type.FunctionType inner) {
                return inner;
            }
        }
        return t;
    }

    /** The structural function type a value of {@code t} has, if it is a
     * function value at all: a bare function type, a carrier around one, or
     * an m3 Function subclass whose instantiated supertype spells one. */
    public java.util.Optional<Type.FunctionType> functionTypeOf(Type t) {
        return deepUnwrapFunction(t) instanceof Type.FunctionType ft
                ? java.util.Optional.of(ft) : java.util.Optional.empty();
    }

    /**
     * {@code t} viewed AS its supertype {@code superFqn}, arguments
     * instantiated: the class's declared supertypes (TypedClass.superTypes,
     * over its own parameters) resolved under the receiver's arguments,
     * walked up until the target class. Parameters the receiver does not
     * supply resolve to Any / [*]. Empty when {@code t} is not nominal or
     * does not reach {@code superFqn}.
     */
    public java.util.Optional<Type> asSuper(Type t, String superFqn) {
        String raw = t instanceof Type.GenericType g ? g.rawFqn()
                : t instanceof Type.ClassType c ? c.fqn() : null;
        if (raw == null) {
            return java.util.Optional.empty();
        }
        if (raw.equals(superFqn)) {
            return java.util.Optional.of(t);
        }
        var cls = ctx.findClass(raw).orElse(null);
        if (cls == null) {
            return java.util.Optional.empty();
        }
        Bindings b = new Bindings();
        List<Type> targs = t instanceof Type.GenericType g ? g.arguments() : List.of();
        List<Multiplicity> margs = t instanceof Type.GenericType g ? g.multArguments() : List.of();
        int pi = 0;
        for (String p : cls.typeParameters()) {
            if (pi < targs.size()) {
                b.bindType(p, targs.get(pi));
            } else if (pi - targs.size() < margs.size()) {
                b.bindMult(p, margs.get(pi - targs.size()));
            } else {
                b.bindType(p, anyType());
                b.bindMult(p, Multiplicity.Bounded.ZERO_MANY);
            }
            pi++;
        }
        for (Type st : cls.superTypes()) {
            Type inst;
            try {
                inst = resolve(st, b);
            } catch (TypeInferenceException e) {
                continue;   // a supertype spelled over something the receiver never binds
            }
            var up = asSuper(inst, superFqn);
            if (up.isPresent()) {
                return up;
            }
        }
        return java.util.Optional.empty();
    }

    /** The lattice FQN of a nominal type ({@code PrecisionDecimal -> Decimal}); {@code null} for non-nominal. */
    private static @com.legend.base.Nullable String nominalFqn(Type t) {
        return switch (t) {
            case Type.Primitive p -> p.qualifiedName();
            case Type.PrecisionDecimal pd -> pd.basePrimitive().qualifiedName();
            case Type.ClassType c -> c.fqn();
            case Type.EnumType e -> e.fqn();
            // a parameterized class is NOMINALLY its raw class (Pair<S,I>
            // meets String at Any, like raw Pair always did)
            case Type.GenericType g -> g.rawFqn();
            default -> null;
        };
    }

    /** Superclass FQNs of {@code fqn}, breadth-first (nearest ancestors first), walking the class lattice. */
    /** The unique candidate whose class formals sit nearest in each
     * argument's linearized supertype order; null when none is unique or
     * a formal is not a plain class. */
    private @com.legend.base.Nullable TypedFunction nearestInLinearization(
            List<TypedFunction> cands, List<ExprType> args) {
        TypedFunction best = null;
        long bestRank = Long.MAX_VALUE;
        boolean tie = false;
        List<List<TypedParameter>> shapesSeen = new ArrayList<>();
        for (TypedFunction c : cands) {
            // a SAME-SHAPE duplicate (a module twin of a native) is the
            // duplicate-signature tolerance's case: the first spelling stands
            if (shapesSeen.contains(c.parameters())) {
                continue;
            }
            shapesSeen.add(c.parameters());
            long rank = 0;
            for (int i = 0; i < args.size() && i < c.parameters().size(); i++) {
                Type formal = c.parameters().get(i).type();
                if (formal.equals(args.get(i).type())) {
                    continue;   // an exact formal (String against String) ranks 0
                }
                String actualRaw = nominalFqn(args.get(i).type());
                if (actualRaw == null) {
                    return null;
                }
                if (!(formal instanceof Type.ClassType fc)) {
                    rank = Long.MAX_VALUE / 2;   // not a class formal: least specific
                    continue;
                }
                if (fc.fqn().equals(actualRaw)) {
                    continue;
                }
                int at = ancestorsOf(actualRaw).indexOf(fc.fqn());
                if (at < 0) {
                    rank = Long.MAX_VALUE / 2;   // accepts by some other rule: least specific
                    continue;
                }
                rank += at + 1;
            }
            if (rank < bestRank) {
                bestRank = rank;
                best = c;
                tie = false;
            } else if (rank == bestRank) {
                tie = true;
            }
        }
        return tie ? null : best;
    }

    private List<String> ancestorsOf(String fqn) {
        List<String> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Deque<String> work = new ArrayDeque<>();
        work.add(fqn);
        while (!work.isEmpty()) {
            String f = work.poll();
            for (String sup : ctx.findClass(f).map(TypedClass::superClassFqns).orElse(List.of())) {
                if (seen.add(sup)) {
                    out.add(sup);
                    work.add(sup);
                }
            }
        }
        return out;
    }

    /** The union of two row-structs, keeping first-seen order; a repeated name must agree. */
    private static Type.RelationType unionRows(Type.RelationType a, Type.RelationType b) {
        List<Type.Column> merged = new ArrayList<>(a.columns());
        for (Type.Column c : b.columns()) {
            Type.Column prior = merged.stream().filter(m -> sameColumn(m.name(), c.name())).findFirst().orElse(null);
            if (prior == null) {
                merged.add(c);
            } else if (!prior.equals(c)) {
                throw new TypeInferenceException("column '" + c.name() + "' appears with conflicting types");
            }
        }
        return new Type.RelationType(merged);
    }

    /** {@code Any} as a type value &mdash; the lattice top. */
    public static Type anyType() {
        return new Type.ClassType(ANY_FQN);
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    /** Subtype check over the primitive lattice (precision/width-agnostic, §3.2). */
    private boolean isPrimitiveSubtype(Type actual, Type formal) {
        String actualFqn = primitiveFqn(actual);
        String formalFqn = primitiveFqn(formal);
        if (actualFqn == null || formalFqn == null) {
            return false;
        }
        return actualFqn.equals(formalFqn) || ctx.isSubtype(actualFqn, formalFqn);
    }

    /** The lattice FQN a primitive-ish type collapses to ({@code PrecisionDecimal -> Decimal}). */
    private static @com.legend.base.Nullable String primitiveFqn(Type t) {
        if (t instanceof Type.Primitive p) {
            return p.qualifiedName();
        }
        if (t instanceof Type.PrecisionDecimal pd) {
            return pd.basePrimitive().qualifiedName();
        }
        return null;
    }

    /** A re-bind is OK only if it matches, or either side is {@code Any} (the escape hatch). */
    private boolean compatibleRebind(Type existing, Type actual) {
        if (isAny(existing) || isAny(actual)) {
            return true;
        }
        // a RAW reference to a parameterized class (EnumerationMapping) is
        // that class over Any (real pure): it re-binds against any
        // instantiation of the same raw class
        if (rawOfSameClass(existing, actual)) {
            return true;
        }
        if (existing instanceof Type.Primitive || existing instanceof Type.PrecisionDecimal) {
            String a = primitiveFqn(actual), e = primitiveFqn(existing);
            return a != null && a.equals(e);   // precision-agnostic
        }
        // An UNSOLVED schema fragment (every column type still `?`) re-binding against an
        // already-bound row: compatible iff its column names all exist there — this is how
        // over(~city)'s _Window<(city:?)> meets extend's _Window<T> with T bound to the
        // source row, and the containment check IS the partition/sort-column validation.
        Type.RelationType exView = Type.schemaView(existing);
        if (exView != null && actual instanceof Type.RelationType ar
                && isUnknownFragment(ar)) {
            return ar.columns().stream().allMatch(c ->
                    exView.columns().stream().anyMatch(e -> sameColumn(e.name(), c.name())));
        }
        // Relation identity is the COLUMNS — dynamicColumns (pivot templates)
        // are executor metadata; a template-carrying schema re-binding against
        // its template-less rebuild must not spuriously conflict (audit).
        // Applies to bare schemas and wrapped tables alike (schema view);
        // a ROW never silently rebinds against a TABLE — same columns or
        // not, the kinds differ (loud at the caller).
        if (existing instanceof Type.RelationType er2 && actual instanceof Type.RelationType ar2) {
            return er2.columns().equals(ar2.columns());
        }
        Type.RelationType ew = Type.relationSchema(existing);
        Type.RelationType aw = Type.relationSchema(actual);
        if (ew != null && aw != null) {
            return ew.columns().equals(aw.columns());
        }
        return existing.equals(actual);
    }

    private static boolean isAny(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(ANY_FQN);
    }

    private static TypeInferenceException fail(Type formal, Type actual) {
        return new TypeInferenceException("expected " + formal.typeName() + ", got " + actual.typeName());
    }

    /** The one candidate whose parameter types are pairwise at least as
     * specific as every other's, strictly more specific than each in at
     * least one class-typed position; null when no such candidate. */
    private @com.legend.base.Nullable TypedFunction mostSpecific(List<TypedFunction> cands) {
        TypedFunction best = null;
        for (TypedFunction c : cands) {
            boolean beatsAll = true;
            for (TypedFunction o : cands) {
                if (o == c) {
                    continue;
                }
                if (!moreSpecific(c, o)) {
                    beatsAll = false;
                    break;
                }
            }
            if (beatsAll) {
                if (best != null) {
                    return null;
                }
                best = c;
            }
        }
        return best;
    }

    private boolean moreSpecific(TypedFunction a, TypedFunction b) {
        boolean strict = false;
        for (int i = 0; i < a.parameters().size() && i < b.parameters().size(); i++) {
            Type pa = a.parameters().get(i).type();
            Type pb = b.parameters().get(i).type();
            if (!(pa instanceof Type.ClassType ca) || !(pb instanceof Type.ClassType cb)) {
                if (!pa.equals(pb)) {
                    return false;
                }
                continue;
            }
            if (ca.fqn().equals(cb.fqn())) {
                continue;
            }
            if (ctx.isSubtype(ca.fqn(), cb.fqn())) {
                strict = true;
            } else {
                return false;
            }
        }
        return strict;
    }


}
