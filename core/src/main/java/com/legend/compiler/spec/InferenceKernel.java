package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedClass;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.Multiplicity;
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
 * <h2>Relation representation (G-&alpha;)</h2>
 * A relation type appears two ways: as the signature form
 * {@code GenericType(Relation, [row])} and as the computed-value form &mdash; a
 * <strong>bare</strong> {@link Type.RelationType} row-struct. The dedicated
 * relation case in {@link #unify} bridges the two (binding the schema variable
 * to the bare row-struct), and {@link #resolve} <em>unwraps</em>
 * {@code Relation<row>} back to the bare row-struct value form.
 */
public final class InferenceKernel {

    private static final String RELATION_FQN = Pure.RELATION.qualifiedName();
    private static final String ANY_FQN = Pure.ANY.qualifiedName();
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
    public void unify(Type formal, Type actual, Bindings b) {
        // Function<{...}> is the WRAPPED spelling of a bare FunctionType —
        // signatures use the wrapper, lambda-typed values may carry the bare
        // form. Normalize BOTH sides so the FunctionType arm sees one shape.
        Type ff = unwrapFunction(formal);
        Type fa = unwrapFunction(actual);
        if (ff != formal || fa != actual) {
            unify(ff, fa, b);
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
                if (!(actual instanceof Type.EnumType ae && ae.fqn().equals(e.fqn()))) {
                    throw fail(formal, actual);
                }
            }
            case Type.ClassType c -> {
                // SUBTYPE conformance (a Person flows into an Employee-typed
                // param's superclass) — matching what overload SCORING already
                // accepts (paramTypeScore scores subtypes as matches); the two
                // halves of one kernel must agree, or resolution selects a
                // winner unification then rejects (audit finding).
                if (!(actual instanceof Type.ClassType ac
                        && (ac.fqn().equals(c.fqn()) || ctx.isSubtype(ac.fqn(), c.fqn())))) {
                    throw fail(formal, actual);
                }
            }

            // Dedicated relation case: bind the schema var to the actual's bare
            // row-struct (whether the actual is wrapped Relation<row> or bare).
            case Type.GenericType g when g.rawFqn().equals(RELATION_FQN) -> {
                Type row = relationRow(actual);
                if (row == null) {
                    throw new TypeInferenceException("expected a Relation, got " + actual.typeName());
                }
                unify(g.arguments().get(0), row, b);
            }
            case Type.GenericType g -> {
                if (!(actual instanceof Type.GenericType ag
                        && ag.rawFqn().equals(g.rawFqn())
                        && ag.arguments().size() == g.arguments().size())) {
                    throw fail(formal, actual);
                }
                for (int i = 0; i < g.arguments().size(); i++) {
                    unify(g.arguments().get(i), ag.arguments().get(i), b);
                }
            }

            case Type.RelationType r -> {
                if (!(relationRow(actual) instanceof Type.RelationType ar)) {
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
                        unify(formalParam, af.params().get(i).type(), b);
                        unifyMult(f.params().get(i).multiplicity(),
                                af.params().get(i).multiplicity(),
                                af.params().get(i).type(), b);
                    }
                } finally {
                    b.exitContravariant();
                }
                unify(f.result().type(), af.result().type(), b);
                unifyMult(f.result().multiplicity(), af.result().multiplicity(),
                        af.result().type(), b);
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
        if (!(relationRow(actual) instanceof Type.RelationType actualRow)) {
            throw new TypeInferenceException("expected a column specification (a row-struct), got "
                    + actual.typeName());
        }
        switch (sa.op()) {
            case SUBSET -> {
                Type right = resolve(sa.right(), b);   // param order guarantees T is bound
                if (!(right instanceof Type.RelationType schema)) {
                    throw new TypeInferenceException("⊆ right-hand side is not a relation schema: "
                            + right.typeName());
                }
                List<Type.Column> selected = new ArrayList<>(actualRow.columns().size());
                for (Type.Column c : actualRow.columns()) {
                    selected.add(schema.columns().stream()
                            .filter(sc -> sc.name().equals(c.name()))
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
                if (concrete.columns().size() != 1) {
                    throw new TypeInferenceException("expected ONE column, got "
                            + concrete.typeName());
                }
                Type want = wildcard.columns().get(0).type();
                Type got = concrete.columns().get(0).type();
                if (!conformsForWildcard(got, want)) {
                    throw new TypeInferenceException("column '"
                            + concrete.columns().get(0).name() + "' has type "
                            + got.typeName() + " but the constraint requires "
                            + want.typeName());
                }
            }
            default -> throw new TypeInferenceException(
                    "unsupported ⊆ left-hand side: " + left.typeName());
        }
    }

    /** Wildcard-column type conformance: exact, or within the numeric family. */
    private boolean conformsForWildcard(Type got, Type want) {
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
            Type.Column prior = merged.stream().filter(m -> m.name().equals(c.name())).findFirst().orElse(null);
            if (prior == null) {
                merged.add(c);
            } else if (!prior.equals(c)) {
                throw new TypeInferenceException("schema variable " + v.name()
                        + " binds column '" + c.name() + "' inconsistently");
            }
        }
        b.bindType(v.name(), new Type.RelationType(merged));
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
            if (!compatibleRebind(existing, actual)) {
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
                if (existing instanceof Type.RelationType er
                        && actual instanceof Type.RelationType ar) {
                    throw new TypeInferenceException("column mismatch: type variable "
                            + v.name() + " bound to relation "
                            + er.columns().stream().map(Type.Column::name).toList()
                            + " cannot also bind relation "
                            + ar.columns().stream().map(Type.Column::name).toList());
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
                    .filter(c -> c.name().equals(fc.name()))
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
     * multiplicity variable, otherwise validates compatibility. Following engine
     * convention, only the {@code [*] -> [1]} case is rejected, and validation is
     * <strong>skipped for relation sources</strong> (§3.2 &mdash; relation ops are
     * typed {@code [*]} but their signatures say {@code [1]}).
     */
    public void unifyMult(Multiplicity formal, Multiplicity actual, Type actualType, Bindings b) {
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
                    // contained range keeps the solution stable).
                    b.bindMult(v.name(), new Multiplicity.Bounded(
                            Math.min(e.lower(), a2.lower()),
                            e.upper() == null || a2.upper() == null ? null
                                    : Math.max(e.upper(), a2.upper())));
                }
            }
            case Multiplicity.Bounded fb -> {
                // Only [*] -> [1] is rejected, and relation sources skip validation (§3.2).
                // A Variant MANY additionally conforms: a collection of
                // variants IS a variant (one JSON array value) — the
                // carrier's own semantics (toMany(@Variant) results flow
                // into to-one column/argument slots as array cells).
                boolean relationSource = relationRow(actualType) != null;
                if (!relationSource && fb.isToOne() && actual.isMany()
                        && !com.legend.compiler.element.type.PlatformTypes.isVariant(actualType)) {
                    throw new TypeInferenceException(
                            "expected at most one value, got many (" + actual.text() + ")");
                }
                // A STATICALLY EMPTY actual ([0..0], the [] literal) never
                // satisfies a required slot — Nil-as-bottom conforms on the
                // TYPE lattice, but multiplicity still binds (abs([]) and
                // f():Integer[1]{[]} are real-pure errors; audit).
                if (!relationSource && fb.lower() >= 1
                        && actual instanceof Multiplicity.Bounded ab
                        && ab.upper() != null && ab.upper() == 0) {
                    throw new TypeInferenceException(
                            "expected at least one value, got none ([0])");
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
    public Type resolve(Type t, Bindings b) {
        return switch (t) {
            // The unknown column type `?` of a colspec VALUE is not a solvable variable —
            // it passes through untouched (⊆/= replace it before it can reach an output schema).
            case Type.TypeVar v when isUnknown(v) -> t;
            case Type.TypeVar v -> b.type(v.name()).orElseThrow(() ->
                    new TypeInferenceException("unbound type variable " + v.name()));

            // Relation<row> -> bare row-struct (the computed-value form, G-alpha).
            case Type.GenericType g when g.rawFqn().equals(RELATION_FQN) && g.arguments().size() == 1 ->
                    resolve(g.arguments().get(0), b);
            case Type.GenericType g -> new Type.GenericType(g.rawFqn(),
                    g.arguments().stream().map(a -> resolve(a, b)).toList());

            case Type.SchemaAlgebra sa -> resolveSchemaAlgebra(sa, b);
            // dynamicColumns (pivot templates) RIDE resolution — rebuilding
            // without them silently degraded downstream pivot column typing
            // to SQL-type derivation (audit finding).
            case Type.RelationType r -> new Type.RelationType(
                    resolveColumns(r.columns(), b), r.dynamicColumns());

            // Leaves: no variables to substitute.
            case Type.Primitive ignored -> t;
            case Type.PrecisionDecimal ignored -> t;
            case Type.ClassType ignored -> t;
            case Type.EnumType ignored -> t;
            case Type.FunctionType ignored -> t;
        };
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
                        if (lr.columns().stream().anyMatch(e -> e.name().equals(c.name()))) {
                            throw new TypeInferenceException("the column '" + c.name()
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
            case Multiplicity.Var v -> b.mult(v.name()).orElseThrow(() ->
                    new TypeInferenceException("unbound multiplicity variable " + v.name()));
            case Multiplicity.Bounded ignored -> m;
        };
    }

    /** The resolved {@code (type, multiplicity)} a call produces (a top-level {@link ExprType}). */
    public ExprType resolveOutput(Type returnType, Multiplicity returnMult, Bindings b) {
        return new ExprType(resolve(returnType, b), resolveMult(returnMult, b));
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
            return resolveChosen(arityMatches.get(0), args, name);
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
            throw new TypeInferenceException("no overload of '" + name
                    + "' structurally matches the argument types");
        }
        if (winners.size() > 1) {
            throw new TypeInferenceException("ambiguous overload of '" + name + "': "
                    + winners.size() + " candidates tie for the argument types");
        }
        return resolveChosen(winners.get(0), args, name);
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
            total += typeScore * 10L + multScore;
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
            total += typeScore * 10L + multScore;
        }
        return total;
    }

    /** Unify the chosen overload's parameters against the args, then resolve its output. */
    private Resolution resolveChosen(TypedFunction c, List<ExprType> args, String name) {
        Bindings b = new Bindings();
        for (int i = 0; i < args.size(); i++) {
            TypedParameter p = c.parameters().get(i);
            try {
                unify(p.type(), args.get(i).type(), b);
                unifyMult(p.multiplicity(), args.get(i).multiplicity(), args.get(i).type(), b);
            } catch (TypeInferenceException e) {
                // Re-raise with CALL CONTEXT — a bare "expected X, got Y"
                // reached corpus users with zero callee info (audit finding).
                throw new TypeInferenceException("in call to '" + name + "', argument "
                        + (i + 1) + ": " + e.getMessage(), e);
            }
        }
        return new Resolution(c, resolveOutput(c.returnType(), c.returnMultiplicity(), b));
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
        return switch (formal) {
            case Type.ClassType c when c.fqn().equals(ANY_FQN) -> 0;
            case Type.TypeVar ignored -> 0;

            case Type.Primitive ignored -> primitiveTypeScore(actual, formal);
            case Type.PrecisionDecimal ignored -> primitiveTypeScore(actual, formal);

            case Type.ClassType fc -> {
                if (!(actual instanceof Type.ClassType ac)) {
                    yield -1;
                }
                yield ac.fqn().equals(fc.fqn()) ? 2 : (ctx.isSubtype(ac.fqn(), fc.fqn()) ? 1 : -1);
            }
            case Type.EnumType fe ->
                    (actual instanceof Type.EnumType ae && ae.fqn().equals(fe.fqn())) ? 2 : -1;

            case Type.GenericType g when g.rawFqn().equals(RELATION_FQN) ->
                    relationRow(actual) != null ? 1 : -1;
            case Type.GenericType g ->
                    (actual instanceof Type.GenericType ag && ag.rawFqn().equals(g.rawFqn())) ? 1 : -1;

            case Type.RelationType ignored -> (relationRow(actual) instanceof Type.RelationType) ? 1 : -1;
            case Type.FunctionType ignored -> (actual instanceof Type.FunctionType) ? 1 : -1;

            // Schema algebra never appears as a parameter type to score against.
            case Type.SchemaAlgebra ignored -> -1;
        };
    }

    private int primitiveTypeScore(Type actual, Type formal) {
        String f = primitiveFqn(formal), a = primitiveFqn(actual);
        if (a == null) {
            return -1;
        }
        return a.equals(f) ? 2 : (ctx.isSubtype(a, f) ? 1 : -1);
    }

    /** Multiplicity specificity: exact=5, then tightness 4/3/2/1, var=0; {@code -1} rejects {@code [*]->[1]}. */
    private int paramMultScore(Multiplicity formal, Multiplicity actual, Type actualType) {
        return switch (formal) {
            case Multiplicity.Var ignored -> 0;
            case Multiplicity.Bounded fb -> {
                if (fb.equals(actual)) {
                    yield 5;
                }
                if (fb.isToOne() && actual.isMany() && relationRow(actualType) == null) {
                    yield -1;   // [*] cannot satisfy a to-one slot (unless a relation source, §3.2)
                }
                // [0] never satisfies a REQUIRED slot — scoring must agree
                // with resolveChosen's rejection, or selection picks the
                // [1]-param overload over a [0..1] sibling and the check
                // then rejects a program the sibling accepts (audit:
                // coalesce('x', [])).
                if (fb.lower() >= 1 && actual instanceof Multiplicity.Bounded ab
                        && ab.upper() != null && ab.upper() == 0
                        && relationRow(actualType) == null) {
                    yield -1;
                }
                yield multiplicityTightness(fb);
            }
        };
    }

    private static int multiplicityTightness(Multiplicity.Bounded m) {
        Integer up = m.upper();
        int lo = m.lower();
        if (lo == 1 && up != null && up == 1) {
            return 4;   // [1]
        }
        if (lo == 0 && up != null && up == 1) {
            return 3;   // [0..1]
        }
        if (lo == 1 && up == null) {
            return 2;   // [1..*]
        }
        if (lo == 0 && up == null) {
            return 1;   // [*]
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
    public Type commonSupertype(Type a, Type b) {
        if (a.equals(b)) {
            return a;
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

    /** {@code Function<{sig}>} unwraps to its bare {@code FunctionType}; everything else passes through. */
    private static Type unwrapFunction(Type t) {
        if (t instanceof Type.GenericType g
                && g.rawFqn().equals(com.legend.compiler.element.type.PlatformTypes.FUNCTION)
                && g.arguments().size() == 1
                && g.arguments().get(0) instanceof Type.FunctionType inner) {
            return inner;
        }
        return t;
    }

    /** The lattice FQN of a nominal type ({@code PrecisionDecimal -> Decimal}); {@code null} for non-nominal. */
    private static String nominalFqn(Type t) {
        return switch (t) {
            case Type.Primitive p -> p.qualifiedName();
            case Type.PrecisionDecimal pd -> pd.basePrimitive().qualifiedName();
            case Type.ClassType c -> c.fqn();
            case Type.EnumType e -> e.fqn();
            default -> null;
        };
    }

    /** Superclass FQNs of {@code fqn}, breadth-first (nearest ancestors first), walking the class lattice. */
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
            Type.Column prior = merged.stream().filter(m -> m.name().equals(c.name())).findFirst().orElse(null);
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

    /** The bare row-struct of a relation value, whether wrapped {@code Relation<row>} or already bare. */
    private static Type relationRow(Type actual) {
        if (actual instanceof Type.GenericType g
                && g.rawFqn().equals(RELATION_FQN)
                && g.arguments().size() == 1) {
            return g.arguments().get(0);
        }
        if (actual instanceof Type.RelationType) {
            return actual;
        }
        return null;
    }

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
    private static String primitiveFqn(Type t) {
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
        if (existing instanceof Type.Primitive || existing instanceof Type.PrecisionDecimal) {
            String a = primitiveFqn(actual), e = primitiveFqn(existing);
            return a != null && a.equals(e);   // precision-agnostic
        }
        // An UNSOLVED schema fragment (every column type still `?`) re-binding against an
        // already-bound row: compatible iff its column names all exist there — this is how
        // over(~city)'s _Window<(city:?)> meets extend's _Window<T> with T bound to the
        // source row, and the containment check IS the partition/sort-column validation.
        if (existing instanceof Type.RelationType er && actual instanceof Type.RelationType ar
                && (isUnknownFragment(ar) || ar.columns().isEmpty())) {
            // An EMPTY fragment (frame-only over()'s _Window row) constrains
            // nothing — containment over zero columns is vacuous.
            return ar.columns().stream().allMatch(c ->
                    er.columns().stream().anyMatch(e -> e.name().equals(c.name())));
        }
        if (existing instanceof Type.RelationType er0 && er0.columns().isEmpty()
                && actual instanceof Type.RelationType) {
            return true;   // empty EXISTING fragment: the actual row wins downstream
        }
        // Relation identity is the COLUMNS — dynamicColumns (pivot templates)
        // are executor metadata; a template-carrying schema re-binding against
        // its template-less rebuild must not spuriously conflict (audit).
        if (existing instanceof Type.RelationType er2 && actual instanceof Type.RelationType ar2) {
            return er2.columns().equals(ar2.columns());
        }
        return existing.equals(actual);
    }

    private static boolean isAny(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(ANY_FQN);
    }

    private static TypeInferenceException fail(Type formal, Type actual) {
        return new TypeInferenceException("expected " + formal.typeName() + ", got " + actual.typeName());
    }
}
