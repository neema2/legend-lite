// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedAggColSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.compiler.spec.typed.TypedAggColSpecArray;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedAsOfJoin;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCLatestDate;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCTime;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCollectionRelation;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedMatchRuntime;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRawSqlRelation;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSortInfo;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedWrite;

/**
 * The TYPED collection-lane decision (COMPILER_SHORTCUT_AUDIT §1a,
 * Blocker 2): {@code toOne}/{@code toOneMany} pick their checked lane
 * from the OPERAND'S TYPED PROVENANCE, never by sniffing the SQL they
 * just emitted.
 *
 * <p>VALUE lane (pure raising semantics — size != bound raises in the
 * database with pure's message): collections whose provenance is
 * expression-space — literals, natives over value collections,
 * if-branches, and lane-preserving transforms. ROW lane (the engine's
 * relational {@code processNoOp} flow, ADJUDICATED: SQL cannot tell a
 * NULL cell from an empty): anything rooted in a store/relation read.
 *
 * <p>THE SWITCH IS EXHAUSTIVE over the sealed hierarchy — the
 * DEEP_AUDIT_2026_08_21 caught the first draft's {@code default ->
 * false} whitelist missing {@code TypedLimit} (a working
 * {@code take(1)->toOne()} became a compile abort), the exact
 * blind-spot class the whitelist replaced. javac is now the referee: a
 * NEW node type refuses to compile until it is classified here.
 */
final class CollectionLanes {

    private CollectionLanes() {
    }

    /** Is this typed collection VALUE-lane (pure raising semantics)? */
    static boolean valueLane(TypedSpec spec) {
        return switch (spec) {
            // ---- literals & literal-ish leaves: VALUE ----
            case TypedCollection c -> c.elements().stream()
                    .allMatch(CollectionLanes::valueLane);
            case TypedCInteger ignored -> true;
            case TypedCFloat ignored -> true;
            case TypedCDecimal ignored -> true;
            case TypedCString ignored -> true;
            case TypedCBoolean ignored -> true;
            case TypedCDate ignored -> true;
            case TypedCTime ignored -> true;
            case TypedCLatestDate ignored -> true;
            case TypedEnumValue ignored -> true;
            case TypedNewInstance ignored -> true;
            case TypedCopyInstance ignored -> true;
            case TypedTypeRef ignored -> true;
            case TypedPackageableRef ignored -> true;
            // compile-time reflection carrier — folds before lowering
            case com.legend.compiler.spec.typed.TypedDeactivate ignored -> true;
            // ---- lane-preserving transforms: the SOURCE decides ----
            case TypedIf i -> valueLane(PureSql.thunkBody(i.thenBranch()))
                    && i.elseBranch()
                            .map(e -> valueLane(PureSql.thunkBody(e)))
                            .orElse(true);
            case TypedFrom f -> valueLane(f.source());
            case TypedFilter f -> valueLane(f.source());
            case TypedMap m -> valueLane(m.source());
            case TypedCast c -> valueLane(c.source());
            // meta::json navigation: the SOURCE decides; a result envelope is
            // one scalar string value
            case com.legend.compiler.spec.typed.TypedJsonAccess ja -> valueLane(ja.source());
            case com.legend.compiler.spec.typed.TypedJsonResult ignored -> true;
            case TypedSlice s -> valueLane(s.source());
            case TypedSort s -> valueLane(s.source());
            case TypedSortBy s -> valueLane(s.source());
            // census carrier: folded to instance literals BEFORE lowering
            // (StatementExecutor); its result is a VALUE — defensive true
            case com.legend.compiler.spec.typed.TypedCsvCensus c -> true;
            case com.legend.compiler.spec.typed.TypedTestDataGen g -> true;
            case TypedDistinct d -> valueLane(d.source());
            case TypedDrop d -> valueLane(d.source());
            // take()/limit() — the DEEP_AUDIT catch: the whitelist had
            // no arm and a working query aborted the compile
            case TypedLimit l -> valueLane(l.source());
            case TypedConcatenate c ->
                    valueLane(c.left()) && valueLane(c.right());
            // a native call is value-lane iff every MANY-stamped data
            // argument is (vacuously true for scalar-built collections:
            // range, split) AND every ZERO-PARAM thunk's body is —
            // if() arrives as a NATIVE with thunk lambdas (thunks ARE
            // value sources); parameterized lambdas (filter/map element
            // functions) stay excluded — their collection arg decides.
            case TypedNativeCall nc -> nc.args().stream().allMatch(a ->
                    a instanceof TypedLambda l
                            ? l.parameters().isEmpty()
                                    ? valueLane(l.body()
                                            .get(l.body().size() - 1))
                                    : true
                            : !a.info().multiplicity().isMany()
                                    || valueLane(a));
            // ---- store/relation-rooted reads and relation ops: ROW
            // (the engine's processNoOp flow) ----
            case TypedPropertyAccess ignored -> false;
            case TypedVariable ignored -> false;
            case TypedGetAll ignored -> false;
            case TypedMilestonedAccess ignored -> false;
            case TypedNavigate ignored -> false;
            case TypedTableReference ignored -> false;
            case TypedRawSqlRelation ignored -> false;
            case com.legend.compiler.spec.typed.TypedFrameRef ignored -> false;
            case TypedCollectionRelation ignored -> false;
            case TypedTds ignored -> false;
            case TypedProject ignored -> false;
            case TypedSelect ignored -> false;
            case TypedRename ignored -> false;
            case TypedExtend ignored -> false;
            case TypedExtendAgg ignored -> false;
            case TypedExtendWindow ignored -> false;
            case TypedGroupBy ignored -> false;
            case TypedAggregate ignored -> false;
            case TypedPivot ignored -> false;
            case TypedFlatten ignored -> false;
            case TypedJoin ignored -> false;
            case TypedJoinSlot ignored -> false;
            case com.legend.compiler.spec.typed.TypedViewRelation ignored -> false;
            case TypedAsOfJoin ignored -> false;
            case TypedOver ignored -> false;
            case TypedWrite ignored -> false;
            case TypedGraphFetch ignored -> false;
            case TypedSerialize ignored -> false;
            case TypedSerializeGraph ignored -> false;
            case TypedNewInstanceCast ignored -> false;
            case TypedSourceUrl ignored -> false;
            // ---- opaque evaluation / binder machinery: conservative
            // ROW (the inliner reduces the common forms before the
            // rules run — probed: let/eval raise correctly) ----
            case TypedUserCall ignored -> false;
            case TypedEval ignored -> false;
            case TypedLet ignored -> false;
            case TypedLambda ignored -> false;
            case TypedMatch ignored -> false;
            case TypedMatchRuntime ignored -> false;
            case TypedFold ignored -> false;
            // spec-fragment carriers — never a collection operand
            case TypedColSpec ignored -> false;
            case TypedColSpecArray ignored -> false;
            case TypedSortInfo ignored -> false;
            case TypedFuncColSpec ignored -> false;
            case TypedFuncColSpecArray ignored -> false;
            case TypedAggColSpec ignored -> false;
            case TypedAggColSpecArray ignored -> false;
        };
    }

    /** The §5 rule at a VALUE-LANE consumer: positional/counting reads
     * (size/at/indexOf — the ops SQL does not null-skip) consume the
     * COMPACTED carrier, because pure collections hold no empties and a
     * literal of {@code [0..1]} reads carries NULL slots for the empty
     * ones. Identity on definite lists; engine-TEXT renders the wrapper
     * verbatim (no golden movement). Row-lane operands ride through —
     * their carriers compact at the collect (Blocker 1). */
    static com.legend.sql.SqlExpr compactIfValueLane(TypedSpec typedOp,
            com.legend.sql.SqlExpr arg) {
        return valueLane(typedOp) && !scalarCarriedIf(typedOp)
                ? new com.legend.sql.SqlExpr.CompactList(arg)
                : arg;
    }

    /** A C1-COLLAPSED LITERAL operand ({@code [7]} — a to-one-stamped
     * collection literal lowered as its bare element, DEEP_AUDIT §3):
     * the ONE population that must re-box before a list-consuming
     * emission. A to-one PROPERTY READ must NOT box — the corpus pins
     * its null-guarded scalar arms (testContainsEscapePercentage:
     * {@code comments->contains('%')} over String[0..1] is
     * {@code IS NOT NULL AND strpos(...)}, never list_contains). */
    static boolean c1Literal(TypedSpec t) {
        return t instanceof TypedCollection
                && t.info().multiplicity() instanceof
                        com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.upper() != null && b.upper() <= 1;
    }

    /** An if whose branch thunks are ALL to-one-stamped lowers on the
     * SCALAR carrier (MixedEncoding.lubCase — a bare CASE), a loose
     * {@code [*]} outer stamp notwithstanding: there is no list to
     * count, and the engine compiles exactly this {@code toOne} as the
     * unguarded CASE (corpus witness: the milestoned qualified property
     * {@code if(...->isEmpty(), |'empty', |...)->toOne()} in
     * testIsolationOfMilestoningFiltersReferencedInAllPartsOfIfStmt).
     * The guard rules FLOW these — identity over a scalar value. Typed
     * facts only; never the emitted SQL. */
    static boolean scalarCarriedIf(TypedSpec spec) {
        java.util.List<TypedSpec> branches = switch (spec) {
            case TypedIf i -> i.elseBranch()
                    .map(e -> java.util.List.of(
                            PureSql.thunkBody(i.thenBranch()),
                            PureSql.thunkBody(e)))
                    .orElseGet(() -> java.util.List.of(
                            PureSql.thunkBody(i.thenBranch())));
            case TypedNativeCall nc when nc.callee().qualifiedName()
                    .equals("meta::pure::functions::lang::if") ->
                    nc.args().stream()
                            .filter(a -> a instanceof TypedLambda)
                            .map(PureSql::thunkBody)
                            .toList();
            default -> null;
        };
        return branches != null && !branches.isEmpty()
                && branches.stream().allMatch(b ->
                        b.info().multiplicity() instanceof
                                com.legend.compiler.element.type
                                        .Multiplicity.Bounded bb
                        && bb.upper() != null && bb.upper() <= 1);
    }

    /** NULL-cell membership: the list has at least one NULL element
     * (list_contains(NULL) is never true under three-valued equality —
     * both the ^TDSNull() needle and its wire sentinel route here). */
    static com.legend.sql.SqlExpr nullMembership(com.legend.sql.SqlExpr list) {
        return new com.legend.sql.SqlExpr.Call(com.legend.sql.SqlFn.GREATER,
                java.util.List.of(
                com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_LENGTH,
                        com.legend.sql.SqlExpr.Call.of(
                                com.legend.sql.SqlFn.LIST_FILTER, list,
                                new com.legend.sql.SqlExpr.Lambda(
                                        java.util.List.of("_nv"),
                                        com.legend.sql.SqlExpr.Call.of(
                                                com.legend.sql.SqlFn.IS_NULL,
                                                com.legend.sql.SqlExpr.Column
                                                        .param("_nv", list))))),
                new com.legend.sql.SqlExpr.IntLit(0)));
    }

    /** The collection {@code add(set, value)} / {@code add(set, index,
     * value)} overloads only — {@code date::add(date, Duration)} is
     * DateShifts' (adjust over the Duration value's fields). */
    static java.util.List<com.legend.model.FunctionId> collectionAddKeys() {
        return com.legend.builtin.Pure.AT_COLLECTION_ADD;
    }

    /** firstNotNull(set) — pureToSQLQuery.pure: {@code $set->filter(v |
     * $v != TDSNull)->first()}. A LITERAL collection unrolls to a coalesce
     * over its elements (the literal-flattening doctrine; portable — no
     * list encoding); a computed collection filters the list carrier. The
     * null CELL is by lane: SQL NULL on the plain lane; on the variant lane
     * (an Any-typed / mixed collection) TDSNull is the json null slot
     * (MixedEncoding — TDSNull is DATA). A to-one value is itself. */
    static void registerFirstNotNull(java.util.Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        for (com.legend.model.FunctionId f : com.legend.builtin.Pure.AT_TDS_EXTENSIONS_FIRST_NOT_NULL) {
            rules.put(f, (n, args) -> {
                if (Scalars.isToOne(n.args().get(0))) {
                    return args.get(0);
                }
                com.legend.compiler.element.type.Type elems = n.args().get(0).info().type();
                boolean variant = com.legend.compiler.element.type.PlatformTypes.isAny(elems)
                        || com.legend.compiler.element.type.PlatformTypes.isVariant(elems);
                SqlExpr coll = args.get(0) instanceof SqlExpr.CompactList cl ? cl.list() : args.get(0);
                if (coll instanceof SqlExpr.ArrayLit al) {
                    // a LITERAL collection: a ^TDSNull() element is known at
                    // compile time and drops (the static fold the spec body's
                    // filter takes over a literal); a computed element on the
                    // variant lane may hold the json null slot at run time
                    java.util.List<TypedSpec> typedEls = n.args().get(0) instanceof TypedCollection tc
                            && tc.elements().size() == al.elements().size()
                            ? tc.elements() : null;
                    java.util.List<SqlExpr> slots = new java.util.ArrayList<>();
                    for (int i = 0; i < al.elements().size(); i++) {
                        TypedSpec te = typedEls == null ? null : typedEls.get(i);
                        if (te instanceof TypedNewInstance ni && com.legend.compiler.element.type
                                .PlatformTypes.TDS_NULL_FQN.equals(ni.classFqn())) {
                            continue;
                        }
                        boolean literal = te instanceof TypedCInteger || te instanceof TypedCFloat
                                || te instanceof TypedCDecimal || te instanceof TypedCString
                                || te instanceof TypedCBoolean || te instanceof TypedCDate;
                        SqlExpr el = al.elements().get(i);
                        slots.add(variant && !literal ? nullifyJsonNull(el) : el);
                    }
                    return slots.isEmpty() ? new SqlExpr.NullLit()
                            : slots.size() == 1 ? slots.get(0)
                            : new SqlExpr.Call(SqlFn.COALESCE, slots);
                }
                SqlExpr x = SqlExpr.Column.param("x", coll);
                SqlExpr notNull = variant
                        ? new SqlExpr.Call(SqlFn.AND, java.util.List.of(
                                new SqlExpr.Call(SqlFn.IS_NOT_NULL, java.util.List.of(x)),
                                new SqlExpr.Call(SqlFn.NOT_EQUAL, java.util.List.of(
                                        SqlExpr.Call.of(SqlFn.JSON_TYPE, x),
                                        new SqlExpr.StringLit("NULL")))))
                        : new SqlExpr.Call(SqlFn.IS_NOT_NULL, java.util.List.of(x));
                // a computed collection IS find(set, v | v != TDSNull): the
                // find rule owns the carrier emission (no new list site)
                return java.util.Objects.requireNonNull(rules.get(
                        com.legend.builtin.Pure.AT_COLLECTION_FIND.get(0)))
                        .apply(n, java.util.List.of(coll,
                                new SqlExpr.Lambda(java.util.List.of("x"), notNull)));
            });
        }
    }

    /** A variant-lane element whose value is the json null slot reads as
     * SQL NULL (the coalesce sees the cell as empty). */
    private static SqlExpr nullifyJsonNull(SqlExpr el) {
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(
                new SqlExpr.Call(SqlFn.EQUAL, java.util.List.of(
                        SqlExpr.Call.of(SqlFn.JSON_TYPE, el), new SqlExpr.StringLit("NULL"))),
                new SqlExpr.NullLit())), el);
    }

    /** uniqueValueOnly over a group (collectionExtension.pure): the
     * single distinct value, else empty — CASE WHEN COUNT(DISTINCT x)
     * = 1 THEN MAX(x) END (max of one value IS the value); the 2-arg
     * form's DEFAULT rides as the CASE else. (Moved from {@link Lowerer}
     * at the shape limit.) */
    static SqlExpr uniqueValueOnlyAgg(java.util.List<SqlExpr> extra, SqlExpr value) {
        SqlExpr uvDefault = extra.isEmpty() ? new SqlExpr.NullLit() : extra.get(0);
        if (extra.size() > 1) {
            throw new IllegalStateException("uniqueValueOnly aggregate with "
                    + extra.size() + " extra arguments");
        }
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL,
                        new com.legend.sql.SqlAgg.Reducer(com.legend.sql.SqlAgg.Fn.COUNT,
                                java.util.List.of(value), true, java.util.List.of()),
                        new SqlExpr.IntLit(1)),
                new com.legend.sql.SqlAgg.Reducer(com.legend.sql.SqlAgg.Fn.MAX,
                        java.util.List.of(value), false, java.util.List.of()))),
                uvDefault);
    }
}
