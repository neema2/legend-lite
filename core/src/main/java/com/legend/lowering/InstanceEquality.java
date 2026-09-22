// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.ClassLayouts;
import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * F13c — the in-SQL INSTANCE-EQUALITY arm family: ONE owner of the
 * equality relation — the same canonical renders the verdict layer
 * byte-judges with ({@link CanonicalRenderSql}). D91 armed the KEYED
 * half on every lane ({@code <<equality.Key>>} governs equal()
 * wherever it lowers); the IDENTITY half (eq, keyless equal, the
 * static cross-class fold) still needs the minted {@code __id} and
 * stays verdict-lane-gated.
 *
 * <ul>
 *   <li>{@code eq(a,b)} is IDENTITY — {@code a.__id = b.__id} — with NO
 *       static classifier fold: a supertype-stamped alias of the same
 *       instance still compares TRUE (ids are unique per construction
 *       site, so cross-class ids can never collide).</li>
 *   <li>{@code equal(a,b)}/{@code ==} compares canonical renders (keyed
 *       classes their key tree, keyless their identity); a static
 *       classifier mismatch folds FALSE (the engine's exact-classifier
 *       rule — the witnessed shapes are static-exact).</li>
 *   <li>{@code contains}/{@code in} is {@code equal()} per element
 *       (contains.pure) — membership over canon texts, never the raw
 *       structs (which carry the identity field in this lane).</li>
 * </ul>
 *
 * <p>Null returns = unclaimed shape (no identity field in the layout,
 * unclaimable key tree) — the caller keeps the generic rule. The claim
 * gates are TYPE-ONLY and never lower.
 */
final class InstanceEquality {

    private InstanceEquality() {
    }

    /** Type-only claim gate over the four owned natives. */
    static boolean claims(TypedNativeCall n) {
        if (n.args().size() != 2) {
            return false;
        }
        String callee = n.callee().qualifiedName();
        return switch (callee) {
            case "meta::pure::functions::boolean::eq",
                    "meta::pure::functions::boolean::equal" ->
                    instanceFqn(n.args().get(0)) != null
                            && instanceFqn(n.args().get(1)) != null;
            case "meta::pure::functions::collection::contains" ->
                    instanceFqn(n.args().get(1)) != null
                            && classInstanceFqn(
                                    n.args().get(0).info().type()) != null;
            case "meta::pure::functions::collection::in" ->
                    instanceFqn(n.args().get(0)) != null
                            && classInstanceFqn(
                                    n.args().get(1).info().type()) != null;
            default -> false;
        };
    }

    /** The claimed lowering, or null for an unclaimable shape. */
    static @com.legend.base.Nullable SqlExpr lower(TypedNativeCall n,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        String callee = n.callee().qualifiedName();
        return switch (callee) {
            case "meta::pure::functions::boolean::eq",
                    "meta::pure::functions::boolean::equal" ->
                    equality(n, "meta::pure::functions::boolean::eq"
                            .equals(callee), keysOf, sqlTypeOf, scalar);
            default -> contains(n,
                    "meta::pure::functions::collection::contains"
                            .equals(callee),
                    keysOf, sqlTypeOf, scalar, freshVar);
        };
    }

    private static @com.legend.base.Nullable SqlExpr equality(TypedNativeCall n,
            boolean isEq,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar) {
        TypedSpec l = n.args().get(0);
        TypedSpec r = n.args().get(1);
        String lf = Objects.requireNonNull(instanceFqn(l));
        String rf = Objects.requireNonNull(instanceFqn(r));
        SqlType lt = sqlTypeOf.apply(l.info().type());
        SqlType rt = sqlTypeOf.apply(r.info().type());
        if (isEq) {
            // eq = INSTANCE IDENTITY — needs the minted __id, which
            // rides only the verdict/identity lane; the value lane's
            // plain layouts stay on the generic rule (named residue,
            // census §5)
            if (!hasIdentityField(lt) || !hasIdentityField(rt)) {
                return null;
            }
            return SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlExpr.StructGet(scalar.apply(l),
                            ClassLayouts.SYNTHETIC_ID),
                    new SqlExpr.StructGet(scalar.apply(r),
                            ClassLayouts.SYNTHETIC_ID));
        }
        if (!lf.equals(rf)) {
            // the engine's exact-classifier FALSE fold — witnessed
            // static-exact on the identity lane only; on the value
            // lane a supertype-stamped alias could still be the same
            // runtime class, so the shape stays unclaimed there
            return hasIdentityField(lt) && hasIdentityField(rt)
                    ? new SqlExpr.BoolLit(false) : null;
        }
        EqualityKeys keys = keysOf.apply(l.info().type());
        if (keys == null
                && (!hasIdentityField(lt) || !hasIdentityField(rt))) {
            // keyless equal = identity (needs __id) — value-lane
            // keyless shapes keep the generic rule (D91 residue)
            return null;
        }
        SqlExpr ca = CanonicalRenderSql.instanceEqualityCanon(
                scalar.apply(l), keys, lf, lt);
        SqlExpr cb = CanonicalRenderSql.instanceEqualityCanon(
                scalar.apply(r), keys, rf, rt);
        if (ca == null || cb == null) {
            return null;
        }
        return SqlExpr.Call.of(SqlFn.EQUAL,
                new SqlExpr.Cast(ca, SqlType.Scalar.VARCHAR),
                new SqlExpr.Cast(cb, SqlType.Scalar.VARCHAR));
    }

    private static @com.legend.base.Nullable SqlExpr contains(TypedNativeCall n,
            boolean isContains,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        TypedSpec coll = n.args().get(isContains ? 0 : 1);
        TypedSpec needle = n.args().get(isContains ? 1 : 0);
        String cf = Objects.requireNonNull(
                classInstanceFqn(coll.info().type()));
        String nf = Objects.requireNonNull(instanceFqn(needle));
        SqlType lt = sqlTypeOf.apply(needle.info().type());
        if (!cf.equals(nf)) {
            // engine equal(): classifiers must match — statically FALSE
            // where the shapes are witnessed static-exact (identity
            // lane); unclaimed on the value lane (D91 — see equality())
            return hasIdentityField(lt)
                    ? new SqlExpr.BoolLit(false) : null;
        }
        EqualityKeys keys = keysOf.apply(needle.info().type());
        if (keys == null && !hasIdentityField(lt)) {
            // keyless membership = per-element identity — needs __id
            return null;
        }
        String elemVar = freshVar.get();
        SqlExpr elemCanon = CanonicalRenderSql.instanceEqualityCanon(
                SqlExpr.Column.derived(null, elemVar), keys, cf, lt);
        SqlExpr needleCanon = CanonicalRenderSql.instanceEqualityCanon(
                scalar.apply(needle), keys, cf, lt);
        if (elemCanon == null || needleCanon == null) {
            return null;
        }
        SqlExpr collE = PureSql.asList(scalar.apply(coll), isMany(coll));
        SqlExpr mapped = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, collE,
                new SqlExpr.Lambda(List.of(elemVar),
                        new SqlExpr.Cast(elemCanon, SqlType.Scalar.VARCHAR)));
        return SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlExpr.Membership(
                        new SqlExpr.Cast(needleCanon, SqlType.Scalar.VARCHAR),
                        mapped),
                new SqlExpr.BoolLit(false));
    }

    private static boolean hasIdentityField(SqlType t) {
        return t instanceof SqlType.Struct st && st.fields().stream()
                .anyMatch(f -> ClassLayouts.SYNTHETIC_ID.equals(f.name()));
    }

    private static boolean isMany(TypedSpec s) {
        return s.info().multiplicity() instanceof Multiplicity.Bounded b
                ? b.isMany()
                : true;
    }

    /** The MODEL-class fqn of a [1]-multiplicity instance operand, or
     * null (platform carriers, Any/variant/Nil, collections). */
    private static @com.legend.base.Nullable String instanceFqn(TypedSpec s) {
        if (!(s.info().multiplicity() instanceof Multiplicity.Bounded b)
                || b.lower() != 1 || b.upper() == null || b.upper() != 1) {
            return null;
        }
        return classInstanceFqn(s.info().type());
    }

    /** The MODEL-class fqn of an instance TYPE (any multiplicity), or
     * null (platform carriers, Any/variant/Nil). */
    private static @com.legend.base.Nullable String classInstanceFqn(Type t) {
        if (PlatformTypes.isPairCarrier(t) || PlatformTypes.isListCarrier(t)
                || PlatformTypes.isMapCarrier(t)
                || (t instanceof Type.ClassType ct
                        && (PlatformTypes.isVariant(ct)
                                || PlatformTypes.isAny(ct)
                                || PlatformTypes.isNil(ct)))) {
            return null;
        }
        return EqualityKeys.fqnOf(t);
    }

    /**
     * Equality that is STATICALLY disjoint — real pure's type-aware
     * equality folds FALSE at compile time. Moved from {@code Lowerer}
     * (AT its 3500-line cap) into the equality owner; the
     * class-vs-primitive arm joined when leg 7b R0 let the
     * primitive-extension rows reach it (witness eq.pure:
     * {@code assertFalse(eq(^SideClass(...), 1->cast(@ExtendedInteger)))}).
     * ENUM rules: two DIFFERENT enums, or enum vs a non-string kind —
     * enum values lower as name strings (plangen parity), so
     * enum-vs-STRING equality IS the corpus's deliberate name-comparison
     * convention and stays allowed. CLASS rule: a USER class instance
     * against a primitive is disjoint by kind; platform carriers
     * (List/Pair/Map/TDSNull/Variant) and Any/Nil keep their own
     * comparison lanes (TDSNull rides NullSemantics; an Any operand may
     * hold anything at run time — UNDECIDED, never static FALSE).
     */
    static boolean staticallyDisjoint(List<TypedSpec> args) {
        if (args.size() != 2) {
            return false;
        }
        Type a = args.get(0).info().type();
        Type b = args.get(1).info().type();
        boolean ae = a instanceof Type.EnumType;
        boolean be = b instanceof Type.EnumType;
        if (ae && be) {
            return !((Type.EnumType) a).fqn().equals(((Type.EnumType) b).fqn());
        }
        if (ae != be) {
            Type other = ae ? b : a;
            if (PlatformTypes.isAny(other)
                    || PlatformTypes.isNil(other)
                    || PlatformTypes.isVariant(other)) {
                return false;
            }
            return !(other instanceof Type.Primitive prim
                    && prim == Type.Primitive.STRING);
        }
        boolean ac = instanceLiteral(args.get(0));
        boolean bc = instanceLiteral(args.get(1));
        return ac != bc && (ac ? b : a) instanceof Type.Primitive;
    }

    /** The CLASS side must be an instance-construction LITERAL (the
     * witnessed eq.pure shape), never a class-TYPED expression: the
     * resolver SYNTHESIZES eq nodes as cross-store join conditions
     * whose operands are class-typed navigation placeholders, and a
     * type-based fold silently emptied every XStore qualifier join
     * (the G4 corpus regression, 2026-08-27 — 11 graphFetch rows). */
    private static boolean instanceLiteral(TypedSpec s) {
        return (s instanceof com.legend.compiler.spec.typed.TypedNewInstance
                || s instanceof com.legend.compiler.spec.typed.TypedCopyInstance)
                && userClass(s.info().type());
    }

    /** A user-model class value (the struct lane) — every platform
     * carrier and the undecided tops are excluded above/here. Package
     * visible: the toString default-instance arm keys on the same
     * classification (ONE owner of "is this the struct lane"). */
    static boolean userClass(Type t) {
        return (t instanceof Type.ClassType || t instanceof Type.GenericType)
                && !PlatformTypes.isAny(t) && !PlatformTypes.isNil(t)
                && !PlatformTypes.isVariant(t)
                && !PlatformTypes.isListCarrier(t)
                && !PlatformTypes.isPairCarrier(t)
                && !PlatformTypes.isMapCarrier(t)
                && !(t instanceof Type.ClassType c
                        && c.fqn().equals(PlatformTypes.TDS_NULL_FQN));
    }
}
