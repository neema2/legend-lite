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

    private static final String REMOVE_DUPLICATES = "meta::pure::functions::collection::removeDuplicates";
    private static final String DISTINCT = "meta::pure::functions::collection::distinct";
    private static final String IS_DISTINCT = "meta::pure::functions::collection::isDistinct";
    private static final String INDEX_OF = "meta::pure::functions::collection::indexOf";
    private static final String EQUAL = "meta::pure::functions::boolean::equal";

    /** Type-only claim gate over the owned natives: every operation built on equal() over objects
     *  (docs/OBJECT_IDENTITY_2026_10_20.md §3.3; the engine's distinct, isDistinct and union are removeDuplicates'). */
    static boolean claims(TypedNativeCall n) {
        String callee = n.callee().qualifiedName();
        if (n.args().size() == 1 && (REMOVE_DUPLICATES.equals(callee) || DISTINCT.equals(callee)
                || IS_DISTINCT.equals(callee))) {
            // each element whose equal() value is new: a keyed class's keys, a keyless class's identity
            return isMany(n.args().get(0)) && classInstanceFqn(n.args().get(0).info().type()) != null;
        }
        if (n.args().size() != 2) {
            return false;
        }
        return switch (callee) {
            // equal over collections: the same size, each pair equal(), in order
            case EQUAL -> instanceFqn(n.args().get(0)) != null && instanceFqn(n.args().get(1)) != null
                    || (isMany(n.args().get(0)) || isMany(n.args().get(1)))
                            && classInstanceFqn(n.args().get(0).info().type()) != null
                            && classInstanceFqn(n.args().get(1).info().type()) != null;
            case "meta::pure::functions::boolean::eq" ->
                    instanceFqn(n.args().get(0)) != null
                            && instanceFqn(n.args().get(1)) != null;
            // (a Pair that holds an object too: the object by its keys or its identity, never by every field)
            case INDEX_OF -> isMany(n.args().get(0)) && (instanceFqn(n.args().get(1)) != null
                    && classInstanceFqn(n.args().get(0).info().type()) != null
                    || SideEqualities.holdsAnObjectInASlot(n.args().get(0).info().type())
                    && SideEqualities.holdsAnObjectInASlot(n.args().get(1).info().type()));
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
                    .@com.legend.base.Nullable EqualityKeys> keysOfModel,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        // a key declared Variant: comparing two such objects compares their Variants (register S401), refused by name
        Function<Type, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys> keysOf = t -> {
            com.legend.compiler.element.EqualityKeys k = keysOfModel.apply(t);
            com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(k);
            return k;
        };
        String callee = n.callee().qualifiedName();
        if (EQUAL.equals(callee) && (instanceFqn(n.args().get(0)) == null || instanceFqn(n.args().get(1)) == null)) {
            return collectionsEqual(n, keysOf, sqlTypeOf, scalar, freshVar);
        }
        return switch (callee) {
            case REMOVE_DUPLICATES, DISTINCT -> removeDuplicates(n, keysOf, sqlTypeOf, scalar, freshVar);
            case IS_DISTINCT -> isDistinct(n, keysOf, sqlTypeOf, scalar, freshVar);
            case INDEX_OF -> indexOf(n, keysOf, sqlTypeOf, scalar, freshVar);
            case "meta::pure::functions::boolean::eq",
                    EQUAL ->
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
                    CanonicalRenderSql.trackedId(scalar.apply(l)),
                    CanonicalRenderSql.trackedId(scalar.apply(r)));
        }
        if (!lf.equals(rf)) {
            // (two DECLARED classes are two classes only where each side is built where it stands: a value declared
            // its parent's class holds the class it actually is -- measured 2026-11-28, a child among a list typed
            // its parent's answered false by this fold. Such a pair is compared by its leaves, the class each
            // actually is first, where the two classes have one reading)
            if (!(instanceLiteral(l) && instanceLiteral(r)) && lt instanceof SqlType.Struct ls
                    && rt instanceof SqlType.Struct rs) {
                EqualityKeys lk = keysOf.apply(l.info().type());
                if (SideEqualities.oneReadingOfTwoClasses(ls, lk, rs, keysOf.apply(r.info().type()))) {
                    SqlExpr typed = SideEqualities.sameInstance(scalar.apply(l), ls, scalar.apply(r), rs, lk);
                    if (typed != null) {
                        return typed;
                    }
                }
            }
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
        SqlExpr la = scalar.apply(l);
        SqlExpr ra = scalar.apply(r);
        if (lt instanceof SqlType.Struct ls && rt instanceof SqlType.Struct rs) {
            // by their typed leaves, as a comparison of two whole sides reads them: the class each actually is, then
            // its keys by value or its identity (the edition program's step 6); a leaf with no typed reading (a
            // to-many key) keeps the text of the keys below
            SqlExpr typed = SideEqualities.sameInstance(la, ls, ra, rs, keys);
            if (typed != null) {
                return typed;
            }
        }
        SqlExpr ca = CanonicalRenderSql.instanceEqualityCanon(
                la, keys, lf, lt);
        SqlExpr cb = CanonicalRenderSql.instanceEqualityCanon(
                ra, keys, rf, rt);
        if (ca == null || cb == null) {
            return null;
        }
        return SqlExpr.Call.of(SqlFn.EQUAL,
                new SqlExpr.Cast(ca, SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR),
                new SqlExpr.Cast(cb, SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR));
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
        // the element typed as the collection's (a dialect carrying a struct as JSON reads its fields by their types)
        SqlType et = sqlTypeOf.apply(coll.info().type());
        SqlExpr elemCanon = CanonicalRenderSql.instanceEqualityCanon(
                SqlExpr.Column.of(null, elemVar, et, true, com.legend.sql.OutputCol.Origin.DERIVED), keys, cf, et);
        SqlExpr needleCanon = CanonicalRenderSql.instanceEqualityCanon(
                scalar.apply(needle), keys, cf, lt);
        if (elemCanon == null || needleCanon == null) {
            return null;
        }
        SqlExpr collE = PureSql.asList(scalar.apply(coll), isMany(coll));
        SqlExpr mapped = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, collE,
                new SqlExpr.Lambda(List.of(elemVar),
                        new SqlExpr.Cast(elemCanon, SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR)));
        return SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlExpr.Membership(
                        new SqlExpr.Cast(needleCanon, SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR),
                        mapped),
                new SqlExpr.BoolLit(false));
    }

    /** An object's equal() value, as text: a keyed class's key canon, a keyless class's {@code __id}; null where the
     *  struct carries no identity field (a lane whose objects carry none keeps the generic rule). */
    private static @com.legend.base.Nullable Function<SqlExpr, SqlExpr> equalValue(String cf, Type t, SqlType et,
            Function<Type, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys> keysOf,
            String operation) {
        EqualityKeys keys = keysOf.apply(t);
        // by its typed leaves, as == of two lists reads each instance (step 6B part 2: the text of the keys below
        // kept how each key was written -- measured 2026-11-28, indexOf missed a Decimal key written at another
        // scale); a leaf with no typed reading keeps the text
        Function<SqlExpr, SqlExpr> typed = reducedBy(et, keys);
        if (typed != null) {
            return typed;
        }
        if (!hasIdentityField(et)) {
            return null;
        }
        return ref -> {
            if (keys == null) {
                return CanonicalRenderSql.trackedId(ref);
            }
            SqlExpr canon = CanonicalRenderSql.instanceEqualityCanon(ref, keys, cf, et);
            if (canon == null) {
                throw new com.legend.error.NotImplementedException(operation + " over " + cf
                        + ": its key has no canonical form here");
            }
            return new SqlExpr.Cast(canon, SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR);
        };
    }

    /** The collection's elements' equal() values, in order (a date-time's is its instant: {@code Equalities}' lists
     *  of date-times read through here, step 6B's slice 3, 2026-11-28). */
    static SqlExpr equalValues(SqlExpr xs, SqlType et, Function<SqlExpr, SqlExpr> valueOf,
            Supplier<String> freshVar) {
        String y = freshVar.get();
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, xs, new SqlExpr.Lambda(List.of(y),
                valueOf.apply(SqlExpr.Column.of(null, y, et, true, com.legend.sql.OutputCol.Origin.DERIVED))));
    }

    /** {@code removeDuplicates} (and {@code distinct}, the engine's synonym) over objects: an element is kept where the
     *  first element with its equal() value is itself ({@code removeDuplicatesBy}'s form). */
    private static @com.legend.base.Nullable SqlExpr removeDuplicates(TypedNativeCall n,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        TypedSpec coll = n.args().get(0);
        String cf = Objects.requireNonNull(classInstanceFqn(coll.info().type()));
        SqlType et = sqlTypeOf.apply(coll.info().type());
        Function<SqlExpr, SqlExpr> valueOf = equalValue(cf, coll.info().type(), et, keysOf, "removeDuplicates");
        if (valueOf == null) {
            return null;
        }
        String x = freshVar.get();
        String i = freshVar.get();
        SqlExpr xs = PureSql.asList(scalar.apply(coll), true);
        SqlExpr values = equalValues(xs, et, valueOf, freshVar);
        return SqlExpr.Call.of(SqlFn.LIST_FILTER, xs, new SqlExpr.Lambda(List.of(x, i),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.LIST_POSITION, values,
                        valueOf.apply(SqlExpr.Column.of(null, x, et, true, com.legend.sql.OutputCol.Origin.DERIVED))),
                        SqlExpr.Column.derived(null, i))));
    }

    /** {@code isDistinct} over objects: no two elements equal() (the engine's: removeDuplicates' size is the size). */
    private static @com.legend.base.Nullable SqlExpr isDistinct(TypedNativeCall n,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        TypedSpec coll = n.args().get(0);
        String cf = Objects.requireNonNull(classInstanceFqn(coll.info().type()));
        SqlType et = sqlTypeOf.apply(coll.info().type());
        Function<SqlExpr, SqlExpr> valueOf = equalValue(cf, coll.info().type(), et, keysOf, "isDistinct");
        if (valueOf == null) {
            return null;
        }
        return SqlExpr.Call.of(SqlFn.ALL_DISTINCT,
                equalValues(PureSql.asList(scalar.apply(coll), true), et, valueOf, freshVar));
    }

    /** {@code indexOf} over objects: the 0-based place of the first element equal() to the value, -1 where none is. */
    private static @com.legend.base.Nullable SqlExpr indexOf(TypedNativeCall n,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        TypedSpec coll = n.args().get(0);
        TypedSpec value = n.args().get(1);
        String cf = Objects.requireNonNull(EqualityKeys.fqnOf(coll.info().type()));
        String vf = Objects.requireNonNull(EqualityKeys.fqnOf(value.info().type()));
        SqlType et = sqlTypeOf.apply(coll.info().type());
        SqlType vt = sqlTypeOf.apply(value.info().type());
        Function<SqlExpr, SqlExpr> valueOf = equalValue(cf, coll.info().type(), et, keysOf, "indexOf");
        Function<SqlExpr, SqlExpr> needleOf = equalValue(vf, value.info().type(), vt, keysOf, "indexOf");
        if (valueOf == null || needleOf == null) {
            return null;
        }
        if (!cf.equals(vf)) {
            // equal(): classifiers must match, so no element is the value
            return new SqlExpr.IntLit(-1);
        }
        if (et instanceof SqlType.Struct es && vt instanceof SqlType.Struct vs) {
            // (the collection's layout and the value's, leaf against leaf: the two may hold a key in two SQL types)
            List<List<SideEqualities.Leaf>> both = SideEqualities.pairedLeaves(es, vs, keysOf.apply(value.info()
                    .type()));
            if (both != null) {
                valueOf = ref -> reduced(ref, both.get(0), false);
                needleOf = ref -> reduced(ref, both.get(1), false);
            }
        }
        return SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.COALESCE,
                SqlExpr.Call.of(SqlFn.LIST_POSITION,
                        equalValues(PureSql.asList(scalar.apply(coll), true), et, valueOf, freshVar),
                        needleOf.apply(scalar.apply(value))),
                new SqlExpr.IntLit(0)), new SqlExpr.IntLit(1));
    }

    /** {@code equal} over collections of objects: the same size and each pair equal(), in order (a null operand the
     *  empty collection). */
    private static @com.legend.base.Nullable SqlExpr collectionsEqual(TypedNativeCall n,
            Function<Type, com.legend.compiler.element
                    .@com.legend.base.Nullable EqualityKeys> keysOf,
            Function<Type, SqlType> sqlTypeOf,
            Function<TypedSpec, SqlExpr> scalar,
            Supplier<String> freshVar) {
        TypedSpec l = n.args().get(0);
        TypedSpec r = n.args().get(1);
        String lf = Objects.requireNonNull(classInstanceFqn(l.info().type()));
        String rf = Objects.requireNonNull(classInstanceFqn(r.info().type()));
        SqlType lt = sqlTypeOf.apply(l.info().type());
        SqlType rt = sqlTypeOf.apply(r.info().type());
        Function<SqlExpr, SqlExpr> lv = equalValue(lf, l.info().type(), lt, keysOf, "equal");
        Function<SqlExpr, SqlExpr> rv = equalValue(rf, r.info().type(), rt, keysOf, "equal");
        if (lv == null || rv == null || !lf.equals(rf)) {
            return null;
        }
        SqlExpr empty = new SqlExpr.Cast(CanonicalRenderSql.emptyArray(), new SqlType.Array(SqlType.Scalar.VARCHAR), SqlExpr.Reason.OPERATOR);
        return SqlExpr.Call.of(SqlFn.EQUAL,
                SqlExpr.Call.of(SqlFn.COALESCE, equalValues(PureSql.asList(scalar.apply(l), isMany(l)), lt, lv,
                        freshVar), empty),
                SqlExpr.Call.of(SqlFn.COALESCE, equalValues(PureSql.asList(scalar.apply(r), isMany(r)), rt, rv,
                        freshVar), empty));
    }

    /**
     * TWO SIDES OF INSTANCES OF ONE CLASS THE SAME, AS THE ONE "SAME VALUES" INSTRUCTION (step 6B part 2, objects;
     * the user, 2026-11-28: "reduce each side to what it is compared by, then one instruction"): at least one side a
     * list; each instance -- a model class's or a Pair's -- reduced to what it is compared by, the class it
     * actually is and then its keys by value, or its identity where its class has no key (the leaves
     * {@link SideEqualities#pairedLeaves} names, each read as {@link SideEqualities#leafOf} reads it: a Decimal key
     * by its number, a date-time key by its instant); the instruction, in order or in any order, over the two
     * lists of reduced instances, wherever the comparison stands. Each side's rows ride reduced the same way where
     * the lowering has them ({@code sideRows}: a side that reads nothing of the row it stands in), for a database
     * that compares rows. Until this piece a query's comparison of two such lists was a statement of its own
     * ({@code SideEqualities.instances}), which a list's function cannot hold on a database that runs no sub-query
     * there, and inside a row it was the text of the keys ({@link #collectionsEqual}), which kept how each key was
     * written. Null where the two are no such pair, or a leaf has no typed reading here (a to-many key, a key slot
     * of a polymorphic or Any class, a JSON field, a keyless class with no identity): the caller's other rules
     * answer, or refuse by name.
     *
     * @param bound whether the two sides are bound once to a statement's columns ({@code Lowerer.overSides})
     */
    static @com.legend.base.Nullable SqlExpr sameInstances(TypedNativeCall n, boolean anyOrder,
            Function<Type, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys> keysOfModel,
            Function<Type, SqlType> sqlTypeOf, Function<TypedSpec, SqlExpr> scalar, Supplier<String> freshVar,
            Function<TypedSpec, com.legend.sql.@com.legend.base.Nullable SqlQuery> sideRows,
            java.util.function.Predicate<TypedSpec> tracksIdentity, boolean bound) {
        if (n.args().size() != 2) {
            return null;
        }
        TypedSpec l = n.args().get(0);
        TypedSpec r = n.args().get(1);
        if (!SideEqualities.comparableInstances(l, r)) {
            return null;
        }
        // (a List of instances is its instances: list([a, b]) == list([c, d]) compares the two lists)
        Type lOf = instancesOf(l.info().type());
        Type rOf = instancesOf(r.info().type());
        boolean oneEach = !isMany(l) && !isMany(r) && lOf == l.info().type() && rOf == r.info().type();
        if (oneEach && (anyOrder || instanceFqn(l) != null && instanceFqn(r) != null)) {
            // (two single instances of a model class are equality()'s, which folds two declared classes and reads
            // an identity; hasSameElements compares lists)
            return null;
        }
        // (two single values in a query need no rows: none ride their comparison; bound once, their rows say how
        // each is held)
        // (a List of instances is ONE value: its rows are the row that holds the list, never its instances' --
        // read as theirs, every leaf was no value and two lists of other instances were "the same" on a database
        // that compares rows; measured 2026-11-28)
        boolean aList = lOf != l.info().type() || rOf != r.info().type();
        com.legend.sql.SqlQuery lRows = aList || oneEach && !bound ? null : sideRows.apply(l);
        com.legend.sql.SqlQuery rRows = lRows == null ? null : sideRows.apply(r);
        // (each side's layout as its own rows hold it, where it has rows: what the planner read to stand aside)
        SqlExpr lOne = scalar.apply(l);
        SqlExpr rOne = scalar.apply(r);
        boolean rows = lRows != null && rRows != null;
        SqlType lt = lRows != null && rRows != null ? lRows.outputs().get(0).type() : layoutOf(l, lOne, sqlTypeOf);
        SqlType rt = lRows != null && rRows != null ? rRows.outputs().get(0).type() : layoutOf(r, rOne, sqlTypeOf);
        if (lt instanceof SqlType.Array la) {
            lt = la.element();
        }
        if (rt instanceof SqlType.Array ra) {
            rt = ra.element();
        }
        if (!(lt instanceof SqlType.Struct ls) || !(rt instanceof SqlType.Struct rs)) {
            return null;
        }
        EqualityKeys keys = keysOfModel.apply(lOf);
        com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(keys);
        if (!Objects.equals(EqualityKeys.fqnOf(lOf), EqualityKeys.fqnOf(rOf))
                && !SideEqualities.oneReadingOfTwoClasses(ls, keys, rs, keysOfModel.apply(rOf))) {
            return null;
        }
        List<List<SideEqualities.Leaf>> both = SideEqualities.pairedLeaves(ls, rs, keys);
        if (both == null) {
            return null;
        }
        boolean lTracked = tracksIdentity.test(l);
        boolean rTracked = tracksIdentity.test(r);
        if (oneEach && !listValue(lOne) && !listValue(rOne)) {
            // one value each (two Pairs, two optional instances): every pair of leaves not distinct, as two single
            // instances of a model class are compared (SideEqualities.sameInstance)
            return SideEqualities.sameInstance(lOne, ls, lTracked, rOne, rs, rTracked, keys);
        }
        SqlExpr lv = comparedBy(l, lOne, ls, both.get(0), lTracked, freshVar);
        SqlExpr rv = comparedBy(r, rOne, rs, both.get(1), rTracked, freshVar);
        return new SqlExpr.Call(anyOrder ? SqlFn.SAME_IN_ANY_ORDER : SqlFn.SAME_IN_ORDER,
                lRows == null || rRows == null ? List.of(lv, rv)
                : List.of(lv, rv,
                        new SqlExpr.ScalarSubquery(SideEqualities.leafRowsOf(lRows, both.get(0), lTracked)),
                        new SqlExpr.ScalarSubquery(SideEqualities.leafRowsOf(rRows, both.get(1), rTracked))));
    }

    /**
     * AN INSTANCE AMONG INSTANCES OF ITS CLASS ({@code contains}, {@code in}; step 6B part 2, objects): the value
     * and each of the collection's instances reduced to what an instance is compared by, as {@link #sameInstances}
     * reduces them -- the class it actually is, then its keys by value or its identity -- and the value's a member
     * where some instance's leaves are each not distinct from its own. The collection's rows ride reduced the same
     * way where the lowering has them, for a database with no list membership. Until this piece it was the text of
     * the keys ({@link #contains}), which kept how each key was written. Null where the two are no such pair or a
     * leaf has no typed reading here.
     */
    static @com.legend.base.Nullable SqlExpr memberInstances(TypedNativeCall n,
            Function<Type, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys> keysOfModel,
            Function<Type, SqlType> sqlTypeOf, Function<TypedSpec, SqlExpr> scalar, Supplier<String> freshVar,
            Function<TypedSpec, com.legend.sql.@com.legend.base.Nullable SqlQuery> sideRows,
            java.util.function.Predicate<TypedSpec> tracksIdentity, boolean bound) {
        TypedSpec coll = Equalities.membershipList(n);
        if (coll == null) {
            return null;
        }
        TypedSpec needle = n.args().get(coll == n.args().get(0) ? 1 : 0);
        if (!SideEqualities.comparableInstances(coll, needle) || isMany(needle)) {
            return null;
        }
        com.legend.sql.SqlQuery rows = sideRows.apply(coll);
        com.legend.sql.SqlQuery held = bound ? sideRows.apply(needle) : null;
        SqlExpr list0 = scalar.apply(coll);
        SqlExpr one0 = scalar.apply(needle);
        SqlType ct = rows != null ? rows.outputs().get(0).type() : layoutOf(coll, list0, sqlTypeOf);
        SqlType nt = held != null ? held.outputs().get(0).type() : layoutOf(needle, one0, sqlTypeOf);
        if (ct instanceof SqlType.Array ca) {
            ct = ca.element();
        }
        if (!(ct instanceof SqlType.Struct cs) || !(nt instanceof SqlType.Struct ns)) {
            return null;
        }
        EqualityKeys keys = keysOfModel.apply(needle.info().type());
        com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(keys);
        if (!SideEqualities.instancesOfOneClass(coll, needle)
                && !SideEqualities.oneReadingOfTwoClasses(cs, keysOfModel.apply(coll.info().type()), ns, keys)) {
            return null;
        }
        List<List<SideEqualities.Leaf>> both = SideEqualities.pairedLeaves(cs, ns, keys);
        if (both == null) {
            return null;
        }
        boolean cTracked = tracksIdentity.test(coll);
        boolean nTracked = tracksIdentity.test(needle);
        SqlExpr one = reduced(one0, both.get(1), nTracked);
        SqlExpr list = comparedBy(coll, list0, cs, both.get(0), cTracked, freshVar);
        return SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Membership(one, list,
                rows == null ? null : new SqlExpr.ScalarSubquery(SideEqualities.leafRowsOf(rows, both.get(0),
                        cTracked)), com.legend.sql.SqlTyping.UNKNOWN), new SqlExpr.BoolLit(false));
    }

    /** What ONE instance {@code ref} is compared by: a struct of its leaves ({@code c0, c1, ...}), each read as
     *  {@link SideEqualities#leafOf} reads it. The one reduction every operation built on equal() over objects
     *  reads: {@code ==} of two lists, {@code hasSameElements}, {@code contains}, {@code in}, {@code indexOf},
     *  {@code removeDuplicates}, {@code isDistinct}. */
    private static SqlExpr reduced(SqlExpr ref, List<SideEqualities.Leaf> leaves, boolean tracked) {
        List<SqlExpr.StructLit.Field> fields = new java.util.ArrayList<>();
        for (int i = 0; i < leaves.size(); i++) {
            fields.add(new SqlExpr.StructLit.Field("c" + i, SideEqualities.leafOf(ref, leaves.get(i), tracked),
                    SideEqualities.leafType(leaves.get(i))));
        }
        return new SqlExpr.StructLit(fields);
    }

    /** {@link #reduced} for the instances of ONE layout ({@code removeDuplicates}, {@code isDistinct}); null where a
     *  leaf of it has no typed reading here. */
    private static @com.legend.base.Nullable Function<SqlExpr, SqlExpr> reducedBy(SqlType et,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys keys) {
        if (!(et instanceof SqlType.Struct st)) {
            return null;
        }
        List<List<SideEqualities.Leaf>> both = SideEqualities.pairedLeaves(st, st, keys);
        return both == null ? null : ref -> reduced(ref, both.get(0), false);
    }

    /** The type of the instances a side of type {@code t} holds: a List's are its elements', any other type's its
     *  own. */
    private static Type instancesOf(Type t) {
        return t instanceof Type.GenericType g && PlatformTypes.isListCarrier(t) && g.arguments().size() == 1
                ? g.arguments().get(0) : t;
    }

    /**
     * TWO MAPS TO OBJECTS THE SAME (step 6B part 2): each map as its entries sorted by key
     * ({@link SideEqualities#entriesOf}), each value an object reduced to what it is compared by ({@link #reduced}).
     * Null where the two are not held as maps of one scalar key type to structs whose leaves pair.
     */
    static @com.legend.base.Nullable SqlExpr sameMapsOfInstances(TypedNativeCall n, List<SqlExpr> sides,
            List<com.legend.sql.@com.legend.base.Nullable SqlType> held,
            Function<Type, com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys> keysOfModel) {
        if (!(n.args().get(0).info().type() instanceof Type.GenericType lg) || lg.arguments().size() != 2
                || !(n.args().get(1).info().type() instanceof Type.GenericType rg) || rg.arguments().size() != 2
                || !userClass(lg.arguments().get(1)) || !userClass(rg.arguments().get(1))) {
            return null;
        }
        SqlType lt = held.get(0) != null ? held.get(0) : statedType(sides.get(0));
        SqlType rt = held.get(1) != null ? held.get(1) : statedType(sides.get(1));
        if (!(lt instanceof SqlType.Map lm) || !(rt instanceof SqlType.Map rm) || !lm.key().equals(rm.key())
                || !SideEqualities.scalar(lm.key()) || !(lm.value() instanceof SqlType.Struct ls)
                || !(rm.value() instanceof SqlType.Struct rs)) {
            return null;
        }
        EqualityKeys keys = keysOfModel.apply(lg.arguments().get(1));
        com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(keys);
        if (!Objects.equals(EqualityKeys.fqnOf(lg.arguments().get(1)), EqualityKeys.fqnOf(rg.arguments().get(1)))
                && !SideEqualities.oneReadingOfTwoClasses(ls, keys, rs, keysOfModel.apply(rg.arguments().get(1)))) {
            return null;
        }
        List<List<SideEqualities.Leaf>> both = SideEqualities.pairedLeaves(ls, rs, keys);
        if (both == null) {
            return null;
        }
        return SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                SideEqualities.entriesOf(sides.get(0), lm, v -> reduced(v, both.get(0), false)),
                SideEqualities.entriesOf(sides.get(1), rm, v -> reduced(v, both.get(1), false)));
    }

    /** The type the SQL states for {@code value}, or null where it states none. */
    private static @com.legend.base.Nullable SqlType statedType(SqlExpr value) {
        return value.type() instanceof com.legend.sql.TypeFact.Typed stated ? stated.type() : null;
    }

    /** The layout {@code value}, what {@code side} lowers to, is held in: the type the SQL states for it; where it
     *  states none, a model class's own layout. A Pair's layout is its slots' -- an object in a slot has no layout
     *  of its own outside the Pair that holds it -- so it is read off the value alone (measured 2026-11-28: asked
     *  of the type, a Pair holding an object raised "no SQL type for Pure class"). */
    private static @com.legend.base.Nullable SqlType layoutOf(TypedSpec side, SqlExpr value,
            Function<Type, SqlType> sqlTypeOf) {
        if (value.type() instanceof com.legend.sql.TypeFact.Typed stated) {
            return stated.type();
        }
        return classInstanceFqn(side.info().type()) == null && !SideEqualities.holdsAnObjectInASlot(
                side.info().type()) ? null : sqlTypeOf.apply(side.info().type());
    }

    /** Whether the SQL types {@code value} as a list. */
    private static boolean listValue(SqlExpr value) {
        return value.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array;
    }

    /** {@code value}, the instances {@code side} lowers to (a list, or the one instance of a side that holds one), as
     *  the list of what each is compared by: a struct of its leaves ({@code c0, c1, ...}), an empty slot dropped. */
    private static SqlExpr comparedBy(TypedSpec side, SqlExpr value, SqlType.Struct layout,
            List<SideEqualities.Leaf> leaves, boolean tracked, Supplier<String> freshVar) {
        // (a side bound once to its one list is a list whatever its type says of how many it holds)
        SqlExpr list = CollectionLanes.compacted(side, PureSql.asList(value, isMany(side) || listValue(value)
                || PlatformTypes.isListCarrier(side.info().type())));
        String y = freshVar.get();
        SqlExpr element = SqlExpr.Column.of(null, y, layout, true, com.legend.sql.OutputCol.Origin.DERIVED);
        return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, list, new SqlExpr.Lambda(List.of(y),
                reduced(element, leaves, tracked)));
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
     * null (platform carriers, relations and TDSs, Any/variant/Nil, and a
     * type value: a model element, which travels as its path and compares
     * by it, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9). */
    private static @com.legend.base.Nullable String classInstanceFqn(Type t) {
        String fqn = EqualityKeys.fqnOf(t);
        if (fqn != null && PlatformTypes.isTypeClassifier(fqn)) {
            return null;
        }
        if (PlatformTypes.isPairCarrier(t) || PlatformTypes.isListCarrier(t)
                || PlatformTypes.isMapCarrier(t) || PlatformTypes.isTdsShaped(t)
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
     * (List/Pair/Map/TDSNull) and Any/Nil keep their own
     * comparison lanes (TDSNull rides NullSemantics; an Any operand may
     * hold anything at run time — UNDECIDED, never static FALSE). VARIANT
     * rule (register S401): a Variant against values of other types alone
     * ({@code VariantEquality.between}). The answer is
     * {@link #disjointAnswer}'s.
     */
    static boolean staticallyDisjoint(List<TypedSpec> args) {
        if (args.size() != 2) {
            return false;
        }
        Type a = args.get(0).info().type();
        Type b = args.get(1).info().type();
        // a Variant is never equal to a value of another type, as in Pure (register S401, ruled by the user
        // 2026-11-23), read through casts and collection literals; where only identity decides, refused by name
        if (com.legend.compiler.spec.typed.VariantEquality.between(args.get(0), args.get(1))
                == com.legend.compiler.spec.typed.VariantEquality.Verdict.DISJOINT) {
            return true;
        }
        boolean ae = a instanceof Type.EnumType;
        boolean be = b instanceof Type.EnumType;
        if (ae && be) {
            return !((Type.EnumType) a).fqn().equals(((Type.EnumType) b).fqn());
        }
        if (ae != be) {
            Type other = ae ? b : a;
            if (PlatformTypes.isAny(other)
                    || PlatformTypes.isNil(other)) {
                return false;
            }
            return !(other instanceof Type.Primitive prim
                    && prim == Type.Primitive.STRING);
        }
        boolean ac = instanceLiteral(args.get(0));
        boolean bc = instanceLiteral(args.get(1));
        return ac != bc && (ac ? b : a) instanceof Type.Primitive;
    }

    /** A statically disjoint equality's answer: false, unless both sides may be empty -- then whether both are, as
     *  Pure's {@code [] == []} is true (the audit of 581a93cb and 386db8fe, 2026-11-23: an absent Variant path
     *  against an empty Integer folded false). */
    static SqlExpr disjointAnswer(List<TypedSpec> args, Function<TypedSpec, SqlExpr> lower) {
        if (!com.legend.compiler.spec.typed.VariantEquality.mayBeEmpty(args.get(0))
                || !com.legend.compiler.spec.typed.VariantEquality.mayBeEmpty(args.get(1))) {
            return new SqlExpr.BoolLit(false);
        }
        return Scalars.bothEmpty(args.get(0), lower.apply(args.get(0)), args.get(1), lower.apply(args.get(1)));
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
                        && c.fqn().equals(PlatformTypes.TDS_NULL_FQN))
                // a TYPE value (Type, Class, PrimitiveType, DataType, Enumeration) travels as its path, never as an
                // instance struct: no __id is read off it (D3, 2026-11-13)
                && !(t instanceof Type.ClassType tc && PlatformTypes.isTypeClassifier(tc.fqn()))
                && !(t instanceof Type.GenericType g && PlatformTypes.isTypeClassifier(g.rawFqn()));
    }
}
