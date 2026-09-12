package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ClassLayouts;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedAsOfJoin;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCollectionRelation;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.values.PureDateLiteral;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.legend.lowering.Resolvers.ColumnResolver;
import com.legend.lowering.Resolvers.Resolution;
import com.legend.lowering.Resolvers.UnfoldableRef;
import static com.legend.lowering.Resolvers.Resolution.attempt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
/**
 * Phase I &mdash; relation-pipeline lowering (M2 scope: table/TDS sources +
 * filter/select/rename/sort/slicing/distinct; mappings and class sources are
 * Phase H). One exhaustive dispatch per {@code TypedSpec} kind; every fold
 * decision is {@link Fold}'s; scalar natives are {@link Scalars}'.
 */
public final class Lowerer {

    private int aliasCounter = 0;

    /** The DEFERRED relation-toString registry (dynamic-pivot inners
     * whose '#TDS' column list only exists at the execution boundary):
     * id → the typed relation schema the boundary resolver needs for
     * per-column print forms. The SQL IR node itself is TYPES-FREE
     * (the sql package's standalone wall); this side channel is the
     * lowering layer's own. */
    private final java.util.Map<Integer,
            com.legend.compiler.element.type.Type.RelationType>
            deferredTds = new java.util.LinkedHashMap<>();

    public java.util.Map<Integer,
            com.legend.compiler.element.type.Type.RelationType>
            deferredTds() {
        return deferredTds;
    }
    private int tdsCounter;

    /**
     * The CANONICAL class-value layout resolver (ClassLayouts, supplied by the
     * driver): declared stored properties in declaration order — a struct's
     * fields come from the MODEL, never from an instance's value set. Empty
     * when no model rides along (unit tests over pure-relational queries);
     * class values then keep hitting the loud walls.
     */
    private final Function<Type,
            Optional<List<Type.Column>>> classLayout;

    /** Whether a class FQN exists in the driving model (layoutless-LUB detection). */
    private final Predicate<String> classExists;

    /** The canonical layout of a class-typed value (CollectionRelations). */
    Optional<List<Type.Column>> classLayout(Type t) {
        return classLayout.apply(t);
    }

    /** Layout-typing arm ({@link LayoutTypes}) — carries the
     * recursive-layout cycle guard. */
    private final LayoutTypes layoutTypes;

    public Lowerer() {
        this(t -> Optional.empty(), f -> false);
    }

    public Lowerer(Function<Type,
            Optional<List<Type.Column>>> classLayout,
                   Predicate<String> classExists) {
        this.classLayout = classLayout;
        this.classExists = classExists;
        this.layoutTypes = new LayoutTypes(classLayout, classExists);
    }

    /** SQL type of a value, seeing through class layouts (structs) before {@link PureSql}. */
    SqlType sqlTypeOf(Type t) {
        return layoutTypes.sqlTypeOf(t);
    }

    /**
     * Enclosing lambda scopes for CORRELATED nesting: when a relation query is
     * lowered INSIDE a lambda (a correlated subquery), the outer lambda's
     * resolver is pushed here so the inner predicate can reference outer rows.
     */
    // relationDepth (the F10 3b "lane flag") DELETED 2026-08-24: the
    // slice-1-3 audit found it WRITE-ONLY — no reader ever landed (the
    // hetero-literal claim it was to gate stayed parked), so the
    // counter, its try/finally, and its mutable-field allowlist row
    // were dead state carried by a stale justification (the unwind
    // ledger row the 2026-08-24 review ordered; the boundary IS the
    // lane when the claim returns).

    private final ArrayDeque<ColumnResolver>
            enclosing = new ArrayDeque<>();

    /** The connection's time zone DateTime literals spell in (engine
     * convertDateToSqlString: the dbTimeZone, default GMT) — batch 86. */
    private @com.legend.Nullable String dbTimeZone;

    public Lowerer withDbTimeZone(@com.legend.Nullable String zone) {
        this.dbTimeZone = zone;
        return this;
    }

    /** Query-level let bindings ({@code |let a = ...; ...$a...}), lowered once. */
    private final Map<String, SqlExpr> letBindings = new HashMap<>();

    /** Pre-bind a free variable to an execution-plan TEMPLATE parameter
     * ({@code ${name}} — the plan printer's vocabulary): the variable
     * resolves through the ordinary let-binding channel. */
    /** The two lowering MODES (batch 136): VERBATIM equality under the mapping's
     * own definition (NullSemantics.verbatim); ENGINE-TEXT (CastPolicy). */
    private boolean verbatimEquality;
    private boolean engineText;
    /** The query's execution feature flags (FeatureRules): builder-style like
     *  engineText; LEGACY_SQL_NULL_UNSAFE_EQUALS selects the verbatim equality
     *  form (plain {@code =}) at the four scalar sites. */
    private java.util.Set<com.legend.compiler.spec.typed.Feature> features = java.util.Set.of();
    private boolean legacyNullUnsafeEquals;

    public Lowerer withFeatures(java.util.Set<com.legend.compiler.spec.typed.Feature> features) {
        FeatureRules.requireConsumed(features);
        this.features = features.isEmpty() ? java.util.Set.of() : java.util.EnumSet.copyOf(features);
        this.legacyNullUnsafeEquals = features.contains(
                com.legend.compiler.spec.typed.Feature.LEGACY_SQL_NULL_UNSAFE_EQUALS);
        return this;
    }

    public Lowerer withEngineText() {
        this.engineText = true;
        return this;
    }

    /** Engine-parity join-distinct exists (ExistsJoinForm) — DRIVER
     * opt-in; the standalone-SQL surface keeps lean correlated EXISTS. */
    private boolean engineExistsJoinForm;

    public Lowerer withEngineExistsJoinForm() {
        this.engineExistsJoinForm = true;
        return this;
    }

    /** STREAMING graph root (driver opt-in) — see {@link
     *  StreamingGraphRoot}; nested serializes keep aggregating. */
    private boolean streamingGraphRoot;

    public Lowerer withStreamingGraphRoot() {
        this.streamingGraphRoot = true;
        return this;
    }

    /** F13 site-id minter (driver opt-in, rides an identity layout):
     * {@code __id} mints per construction-site NODE, never per
     * evaluation. Null = a lane with no identity. */
    private @com.legend.Nullable Function<Object, String> instanceIdOf;

    /** F13c/D91 — the driver-supplied {@code <<equality.Key>>} resolver
     * (EqualityKeys.resolve over the model): the in-SQL eq/equal arm
     * compiles instance equality from the SAME canon the verdict layer
     * uses. D91 armed it on EVERY lane — a keyed class's equal() is the
     * key relation wherever it lowers; only the IDENTITY pieces (eq,
     * keyless equal) stay verdict-lane-gated via {@code instanceIdOf}
     * and the {@code __id} layout field. */
    private @com.legend.Nullable Function<Type,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys>
            instanceKeysOf;

    public Lowerer withInstanceIds(Function<Object, String> ids) {
        this.instanceIdOf = ids;
        return this;
    }

    public Lowerer withInstanceKeys(
            Function<Type, com.legend.compiler.element
                    .@com.legend.Nullable EqualityKeys> keys) {
        this.instanceKeysOf = keys;
        return this;
    }

    public Lowerer bindPlanParam(String name, boolean stringTyped) {
        letBindings.put(name, new SqlExpr.PlanParam(name, stringTyped));
        return this;
    }

    public Lowerer bindPlanParam(SqlExpr.PlanParam proto) {
        letBindings.put(proto.name(), proto);
        return this;
    }

    /**
     * Lower a typed QUERY BODY: leading {@code let} statements bind their
     * lowered values into query scope (substitution — the lean output has no
     * trace of the lets); the final statement is the query.
     */
    public SqlQuery lower(List<TypedSpec> body) {
        for (int i = 0; i < body.size() - 1; i++) {
            if (!(body.get(i) instanceof TypedLet let)) {
                throw new IllegalStateException(
                        "only let statements may precede the query expression");
            }
            letBindings.put(let.name(), scalar(let.value(), (var, name) -> {
                throw new IllegalStateException(
                        "a query-level let has no row scope for $" + var);
            }));
        }
        // J-tail: demand-driven subselect column pruning (engine parity —
        // isolated composites enumerate only consumed columns)
        return SubselectPrune.prune(lower(body.get(body.size() - 1)));
    }

    /** Lower a typed query to SQL: relation pipelines and scalar roots. */
    public SqlQuery lower(TypedSpec spec) {
        // from() is execution-context metadata, fully consumed by Phase H —
        // the SQL is its source's. (Relation-typed from-roots take the
        // relation() arm below; a GRAPH-typed root needs the unwrap here.)
        if (spec instanceof TypedFrom from) {
            return lower(from.source());
        }
        // The resolved GRAPH envelope keeps its CLASS-typed info (the
        // result-shape contract) but lowers as a relation.
        if (spec instanceof TypedSerializeGraph g) {
            return conformJsonEgress(serializeGraph(g, streamingGraphRoot));
        }
        // A terminal concatenate is a BARE set operation — no wrapping SELECT *.
        if (spec instanceof TypedConcatenate c) {
            return union(c);
        }
        // ANY relation-ish root — a table, the .rows collection, or ONE
        // ROW (an at()-pick: a one-row TABULAR, matching ResultShape) —
        // lowers through the relation pipeline; a COLLECTION-shaped root
        // filters empty cells at egress (Fold#collectionRootEgress).
        if (Type.schemaView(spec.info().type()) instanceof Type.RelationType rrt) {
            return Fold.collectionRootEgress(relation(spec), rrt,
                    isMany(spec), this::nextAlias);
        }
        // relation->map(row|scalar) at the ROOT is the single-column
        // projection (pure: a VALUE collection derived from rows; the
        // Executor's COLLECTION shape reads N rows × 1 column). A
        // COLLECTION-VALUED mapper ($r.values — the row's cells) FLATTENS
        // per pure map semantics: project the cell array, then UNNEST.
        if (spec instanceof TypedMap m
                && Type.relationValued(m.source().info())
                && m.mapper() instanceof TypedLambda ml
                && !Type.isRelation(ml.functionType().result().type())) {
            boolean collectionMapper = ValueCollections.isCollectionMapper(ml);
            Multiplicity colMult = ml.functionType().result().multiplicity();
            SqlSelect proj = Fold.conformValueEgress(
                    relation(ValueCollections.valueColumnProject(
                            m.source(), ml, spec.info().type(), colMult)),
                    LiteralSpelling.ValueLane.MAP_CHANNEL);
            // SCALAR-STAMPED cells (C1) are one element per row ALREADY —
            // the explode is identity, and UNNEST(scalar) does not bind.
            boolean scalarCells = ml.body().get(ml.body().size() - 1)
                    .info().multiplicity()
                            instanceof Multiplicity.Bounded cb
                    && cb.upper() != null && cb.upper() <= 1;
            if (!collectionMapper || scalarCells) {
                // NULL-DROP at COLLECTION egress (shortcut audit §5,
                // relation lane): Fold#cellPresentFiltered.
                if (isMany(spec) && Fold.optionalScalarCell(colMult)) {
                    return Fold.cellPresentFiltered(proj, "value",
                            nextAlias());
                }
                return proj;
            }
            String sub = nextAlias();
            return Fold.unnestColumn(new SqlSource.Subselect(proj, sub, null),
                    sub, "value", "value", sqlTypeOf(spec.info().type()));
        }
        return Fold.conformValueEgress(scalarRoot(spec),
                LiteralSpelling.ValueLane.SCALAR_ROOT);
    }

    /**
     * SCALAR result shape: a FROM-less single-value SELECT. Collections and
     * class roots (COLLECTION/GRAPH shapes) are still honestly unbuilt.
     */
    private SqlSelect scalarRoot(TypedSpec spec) {
        SqlExpr e = requiredOneEgress(spec);
        // KIND-FRAGILE root literals swap to kind-carrying spellings
        // (RootLiterals — the D-arc temporal + X-audit decimal rules)
        if (e == null) {
            e = RootLiterals.swap(spec);
        }
        if (e == null) {
            e = scalar(spec, (var, name) -> {
                throw new IllegalStateException("a scalar query has no row scope for $"
                        + var + "." + name);
            });
        }
        // FIX-EMITTER (contract program): an Any-stamped root travels
        // as variant JSON — an inlined body can leave its RAW value
        // here under the callee's declared Any (the letFn wire lie).
        // When the built expr is judged CONCRETE non-JSON, the emitter
        // RECORDS the representation by boxing; Bottom/Unknown never
        // guess (censused, unboxed). Root scope is FROM-less — no
        // column bindings.
        boolean anyStamp = sqlTypeOf(spec.info().type()) == SqlType.Scalar.JSON;
        // read unconditionally: a LITERAL-carried product under ANY
        // stamp (generic/TypeVar dedup results included) must label
        // truthfully — the TREE carries the carrier through
        // element-preserving ops (F10 3b; M3 flip: the stored type IS
        // the authority, the judge is gone from this site)
        com.legend.sql.TypeFact rootJudge = e.type();
        if (anyStamp && !isMany(spec)
                && rootJudge instanceof com.legend.sql.TypeFact.Typed t
                && t.type() != SqlType.Scalar.JSON
                && t.type() != SqlType.Scalar.LITERAL) {
            e = SqlExpr.Call.of(SqlFn.TO_VARIANT, e);
        }
        // F10 slice 2 — the KIND-FAITHFUL CARRIER at a mixed root: a
        // literal collection of >=2 distinct numeric kinds rebuilds as
        // pure-literal spellings (the DOUBLE-promoted array erased
        // Integer 1 into 1.0 here), returning its own Array(LITERAL)
        // construction-site mark.
        SqlExpr mixedLits = LiteralSpelling.mixedNumericArray(spec, e);
        if (mixedLits != null) {
            e = mixedLits;
        }
        // THE ONE LITERAL-LABEL ARM (§4.3, redundant half deleted
        // 2026-08-24): the TREE says whether the finished root carries
        // spellings — every producer marks at construction (the sort
        // arm's cast, mixedNumericArray's mark, LIST_GET/UNNEST
        // flowing the element) and a cast TYPES as its target, so the
        // former cast-mark and mixed-array label arms re-derived what
        // e.type() states. This arm's residual content is the label
        // SPELLING (scalar LITERAL for the element contract) — it
        // retires at M4 with the carrier rule.
        com.legend.sql.SqlType label = sqlTypeOf(spec.info().type());
        if (e.type() instanceof com.legend.sql.TypeFact.Typed jt
                && (jt.type() == SqlType.Scalar.LITERAL
                        || (jt.type() instanceof com.legend.sql.SqlType.Array ja
                                && ja.element() == SqlType.Scalar.LITERAL))) {
            label = SqlType.Scalar.LITERAL;
        }
        // COLLECTION roots explode to N rows (the result-shape contract:
        // Executor reads a collection as N rows x 1 column); the carrier
        // COMPACTS first (audit §5 value lane — a pure collection holds
        // no empties), so egress holds a WALL, not a mask.
        if (isMany(spec)) {
            // a SCALAR-typed value boxes as its one element first
            // (§4bZ-U leg 2, the subagg lateral): list_filter over a
            // bare scalar cannot BIND (DuckDB binder receipt —
            // 'Invalid LIST argument during lambda function binding');
            // [e] is the bindable, null-dropping form and types
            // through (Array(T) -> T at the UNNEST read). JSON stays
            // unboxed — the variant carrier may hold a list itself.
            if (e.type() instanceof com.legend.sql.TypeFact.Typed bt
                    && !(bt.type() instanceof com.legend.sql.SqlType.Array)
                    && bt.type() != SqlType.Scalar.JSON) {
                e = PureSql.asList(e, false);
            }
            e = SqlExpr.Call.of(SqlFn.UNNEST, new SqlExpr.CompactList(e));
        }
        return new SqlSelect(
                List.of(new SqlSelect.Projection(e, "value",
                        new OutputCol("value", label,
                                PureSql.nullable(spec.info().multiplicity())))),
                false, new SqlSource.Dual(),
                null, List.of(), null, null, List.of(), null, null,
                List.of());
    }

    /**
     * EGRESS lower bound (multiplicity audit follow-up, slice A): the
     * engine's Java executor checks the FINISHED result's row count
     * against the declared multiplicity ({@code resultSizeRange}) — the
     * one enforcement its in-expression {@code processNoOp} lane never
     * does. Mid-expression, a row-lane {@code toOne} strips to the bare
     * scalar subquery (empty &rarr; NULL, the ADJUDICATED flow); at the
     * STATEMENT ROOT the row count is still visible in the LIST carrier,
     * so a user {@code toOne} over a MANY-stamped operand keeps the list
     * and guards it: 0 rows raises pure's size-0 cast, 1 row holding
     * NULL extracts NULL (the engine counts rows, not values), N rows
     * raises size-N — all with pure's own message. {@code trustOne}
     * (synthesized conformance) never guards, and [0..1]-stamped
     * operands stay flow-adjudicated (a NULL cell and an empty are
     * indistinguishable there, and the engine flows the NULL cell).
     *
     * @return the guarded root expression, or {@code null} when this is
     *         not the required-one egress shape (caller lowers normally)
     */
    private @com.legend.Nullable SqlExpr requiredOneEgress(TypedSpec spec) {
        if (!(spec instanceof com.legend.compiler.spec.typed.TypedNativeCall tc
                // the recognizer minus its trustOne member: conformance
                // wraps never guard (the C2 provenance split)
                && Pure.isToOneCall(tc.callee().qualifiedName())
                && !Pure.Lite.TRUST_ONE.equals(tc.callee().qualifiedName())
                && !tc.args().isEmpty()
                && tc.args().get(0).info().multiplicity().isMany())) {
            return null;
        }
        SqlExpr op = scalar(tc.args().get(0), (var, name) -> {
            throw new IllegalStateException("a scalar query has no row scope for $"
                    + var + "." + name);
        });
        // only the exact LIST-collecting shape carries an honest row
        // count; anything else (value-lane lists already CheckedOne'd
        // inside the rule, opaque calls) lowers through the normal path.
        // A COMPACTED carrier (audit §5) recognizes through its wrapper;
        // the guard counts the COMPACTED list — pure's null-free size.
        SqlExpr carrier = op instanceof SqlExpr.CompactList cl
                ? cl.list() : op;
        return Scalars.aggStrip(carrier) != null
                ? new SqlExpr.CheckedOne(op) : null;
    }

    String nextAlias() {
        return "t" + aliasCounter++;
    }

    // ==================================================================
    // Relation ops
    // ==================================================================

    SqlSelect relation(TypedSpec spec) {
        // POSITIONAL reads over a relation: at(n) IS slice(n, n+1);
        // first()/head() IS limit 1 — row selection, not value extraction
        if (spec instanceof TypedNativeCall pc
                && !pc.args().isEmpty()
                && Type.relationValued(pc.args().get(0).info())) {
            String fqn = pc.callee().qualifiedName();
            if (fqn.equals("meta::pure::functions::collection::at")
                    && pc.args().size() == 2
                    && pc.args().get(1) instanceof TypedCInteger n) {
                var one = ExprType.one(
                        Type.Primitive.INTEGER);
                return relation(new TypedSlice(
                        pc.args().get(0),
                        new TypedCInteger(
                                n.value().longValue(), one),
                        new TypedCInteger(
                                n.value().longValue() + 1, one),
                        pc.args().get(0).info()));
            }
            if ((fqn.equals("meta::pure::functions::collection::first")
                    || fqn.equals("meta::pure::functions::collection::head"))
                    && pc.args().size() == 1) {
                var one = ExprType.one(
                        Type.Primitive.INTEGER);
                return relation(new TypedLimit(
                        pc.args().get(0),
                        new TypedCInteger(1L, one),
                        pc.args().get(0).info()));
            }
        }
        return switch (spec) {
            case TypedSourceUrl su -> SqlSelect.starOf(
                    new SqlSource.SourceUrl(su.url(), nextAlias(), outputsOf(su.info(), OutputCol.Origin.PHYSICAL)));
            case TypedTableReference t -> SqlSelect.starOf(
                    new SqlSource.Table(t.table(), nextAlias(), outputsOf(t.info(), OutputCol.Origin.PHYSICAL)));

            case TypedTds tds -> tdsLiteral(tds);

            case com.legend.compiler.spec.typed.TypedRawSqlRelation raw ->
                    SqlSelect.starOf(new SqlSource.RawSql(   // Phase 1c
                            raw.sql(), nextAlias(), outputsOf(raw.info(), OutputCol.Origin.DERIVED)));

            case TypedFilter f -> filter(f);

            case TypedSelect sel -> Fold.restrictOverWholeRowDistinct(sel) != null
                    ? distinctNarrowTo(relation(Objects.requireNonNull(
                            Fold.restrictOverWholeRowDistinct(sel)).source()),
                            sel.columns(), sel.info())
                    : narrowTo(relation(sel.source()), sel.columns(), sel.info());

            case TypedDistinct d -> distinct(d);
            // lateral(rel, {row | relationOf(row)}): the lambda's relation
            // body lowers with the row param CORRELATED to the left alias
            // (the enclosing-resolver channel); DuckDB joins it per-row via
            // CROSS JOIN LATERAL. Schema = T+V (checker's schema algebra).
            case TypedNativeCall nc when com.legend.builtin.NativeFn.LowererForm
                    .of(nc.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.LowererForm.LATERAL
                    && nc.args().size() == 2
                    && nc.args().get(1) instanceof TypedLambda lam -> {
                SqlSelect left = relation(nc.args().get(0));
                SqlSource leftSide = asLeftJoinSide(left);
                String param = lam.parameters().get(0);
                ColumnResolver leftCols = (v, name) ->
                        (v == null || v.equals(param))
                                ? resolveOrThrow(SqlSelect.starOf(leftSide), name)
                                : null;
                enclosing.push((v, name) -> {
                    SqlExpr r = leftCols.resolve(v, name);
                    if (r == null) {
                        // NOT this scope's variable: the UnfoldableRef SIGNAL
                        // lets scopedResolver continue outward (a hard throw
                        // severed the whole enclosing chain behind lateral).
                        throw new UnfoldableRef(
                                name == null ? "<whole variable>" : name);
                    }
                    return r;
                });
                SqlSelect right;
                try {
                    right = relation(lam.body().get(lam.body().size() - 1));
                } finally {
                    enclosing.pop();
                }
                SqlSource.Join join = new SqlSource.Join(leftSide,
                        new SqlSource.Subselect(right, nextAlias(), null),
                        SqlSource.Join.Kind.CROSS_LATERAL,
                        null);   // CROSS JOIN takes no ON clause
                // star frame: the join's own outputs (left ++ lateral)
                yield SqlSelect.starOf(join);
            }
            // relation::variant::flatten(collection, ~col): the collection
            // UNNESTs as the single column (real flatten.pure semantics).
            // (the body moved to CollectionRelations at the file guardrail)
            case TypedCollectionRelation cr ->
                    CollectionRelations.flatten(this, cr, List.copyOf(enclosing));
            // a CLASS-typed collection VALUE in relation position (batch 76:
            // range->map->zip feeding project): the relation of its
            // elements' layout fields, in list order
            case TypedSpec cv when CollectionRelations.classValued(this, cv) ->
                    CollectionRelations.explode(this, cv);

            case TypedRename r -> rename(r);

            case TypedSort s -> Sorts.sort(this, s);
            case TypedSortBy sb -> Sorts.sortBy(this, sb);

            case TypedLimit l -> {
                SqlSelect src = relation(l.source());
                yield (Fold.limitFolds(src) ? src : isolate(src)).withLimit(ConstBounds.intOf(l.count()));
            }

            // first()/head() over a RELATION: the first row — LIMIT 1 (the
            // result stays row-typed, one row's TABULAR).
            case TypedNativeCall n when n.args().size() == 1
                    && Type.relationValued(n.args().get(0).info())
                    && (isFamily(n, "first") || isFamily(n, "head")) -> {
                SqlSelect src = relation(n.args().get(0));
                yield (Fold.limitFolds(src) ? src : isolate(src)).withLimit(1L);
            }

            case TypedDrop d -> {
                SqlSelect src = relation(d.source());
                yield (Fold.offsetFolds(src) ? src : isolate(src)).withOffset(ConstBounds.intOf(d.count()));
            }

            case TypedSlice s -> { // literal slice(0,n) IS take(n) — engine processSlice drops a zero fromRow; paginated's COMPUTED (page-1)*size keeps offset 0 (processPaginated pin)
                SqlSelect src = relation(s.source());
                long start = ConstBounds.intOf(s.start());
                SqlSelect base = Fold.offsetFolds(src) ? src : isolate(src);
                yield (start == 0 && s.start() instanceof com.legend.compiler.spec.typed.TypedCInteger ? base : base.withOffset(start)).withLimit(ConstBounds.intOf(s.stop()) - start);
            }

            case TypedGroupBy g -> groupBy(g);
            case TypedNavigate nav -> navigate(nav);

            case TypedAggregate a -> aggregate(a);

            case TypedExtend e -> extend(relation(e.source()), e.columns(), e.info());

            // project over INSTANCE LITERALS (PCT's ^X(...)->project(~[...])):
            // no store exists — each instance becomes one SELECT over a 1-row
            // anchor, its to-many property paths exploding via LEFT JOIN
            // LATERAL UNNEST (cross-product across columns, real pure; LEFT so
            // an empty array NULLs its column instead of killing the row).
            case TypedProject p when VariantShapes.isInstanceLiteral(p.source()) ->
                    InstanceProjection.lower(p, outputsOf(p.info()),
                            this::scalar, noScope(), this::nextAlias,
                            this::sqlTypeOf);

            case TypedProject p -> project(relation(p.source()),
                    p.columns(), p.info(), p.wireForm());

            case TypedConcatenate c -> SqlSelect.starOf(
                    new SqlSource.Subselect(union(c), nextAlias(), null));

            case TypedExtendWindow w -> extendWindow(w);

            case TypedJoin j -> join(j);

            case TypedAsOfJoin aj -> asOfJoin(aj);

            case TypedExtendAgg ea -> extendAgg(ea);

            // from(mapping, runtime): execution-context metadata — a Phase-H
            // concern; the relation flows through unchanged.
            case TypedFrom fr -> relation(fr.source());

            // cast(@Relation<(…)>): when EVERY target column exists in the
            // source, the cast is a real projection — surviving columns only,
            // each SQL-CAST where its type changed (String->Integer). Target
            // names ABSENT from the source are the pivot idiom's dynamic
            // columns — those stay type-only (zero SQL footprint).
            case TypedCast c
                    when Type.relationSchema(c.source().info().type())
                            instanceof Type.RelationType srcRow
                    && Type.relationSchema(c.info().type())
                            instanceof Type.RelationType tgtRow ->
                    relationCast(c, srcRow, tgtRow);

            case TypedFlatten fl -> flatten(fl);

            case TypedPivot pv -> Pivots.lower(this, pv);

            // the Typer's `.rows` MARKER (identity over a relation value —
            // the K result frame's row-index/envelope disambiguator): the
            // resolver erases it on resolved paths; this is the defensive
            // FLOOR for G-direct paths (audit 20c H1 — the marker leaked
            // to this switch's default on the plain compile path).
            case TypedPropertyAccess pa
                    when pa.property().equals(com.legend.compiler
                            .element.type.PlatformTypes.ROWS_MARKER)
                    && Type.isRelation(pa.source().info().type()) ->
                    relation(pa.source());

            // STORE-ONLY nodes: reaching the lowerer is not a missing rule —
            // it means the Phase H resolver failed to rewrite them away. Say
            // so, instead of the frontier default's misdiagnosis.
            case TypedJoinSlot js ->
                    throw new NotImplementedException(
                            "TypedJoinSlot (pipeline slot join '" + js.alias()
                          + "') escaped Phase H store resolution — a resolver gap,"
                          + " not a missing lowering rule");

            case TypedNativeCall nc
                    when RelationPredicates.isRelationIdentity(nc) ->
                    relation(nc.args().get(0));

            case TypedNativeCall nc when ValueCollectionOps.isBareSingleColumnSort(nc) ->
                    Sorts.naturalSort(this, nc);

            case TypedNativeCall nc
                    when ValueCollectionOps.relationDistinct(nc) != null ->
                    distinctOf(nc);

            // SANCTIONED frontier default (root package-info invariant is
            // scoped to hiding-prone switches): the not-yet-lowered TypedSpec
            // variants churn every milestone; each throws LOUD and NAMED.
            default -> throw new NotImplementedException("lowering not yet implemented for "
                    + spec.getClass().getSimpleName()
                    + (spec instanceof TypedNativeCall nc2
                            ? " ('" + nc2.callee().qualifiedName()
                                    + "' in relation position)"
                            : ""));
        };
    }

    /** Nested concatenates flatten into ONE multi-branch union. */
    private SqlUnion union(TypedConcatenate c) {
        List<SqlQuery> branches = new ArrayList<>();
        collectBranches(c, branches);
        List<OutputCol> outs = outputsOf(c.info());
        // a VALUE-typed concatenate carries no relation schema: the
        // union's outputs are its branches' own (SqlUnion.ofBranches)
        return outs.isEmpty() ? SqlUnion.ofBranches(branches, true)
                : new SqlUnion(branches, true, outs);
    }

    private void collectBranches(TypedSpec spec, List<SqlQuery> out) {
        if (spec instanceof TypedConcatenate c) {
            collectBranches(c.left(), out);
            collectBranches(c.right(), out);
        } else {
            out.add(relation(spec));
        }
    }

    /**
     * groupBy: keys + aggregates REPLACE the projection list; the GROUP BY
     * clause carries the key expressions.
     */
    private SqlSelect groupBy(TypedGroupBy g) {
        SqlSelect src = relation(g.source());
        SqlSelect base = Fold.groupByFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "groupBy", b -> buildGroupBy(b, g));
    }

    private SqlSelect buildGroupBy(SqlSelect base0, TypedGroupBy g) {
        // CALENDAR AGGREGATIONS (task G1): calendar natives in agg maps
        // LEFT-join the fiscal calendar table (twice per distinct
        // date/end/type) before the aggs lower over it
        java.util.Map<TypedAggCol, CalendarAgg.Ctx> calCtx =
                new java.util.IdentityHashMap<>();
        SqlSelect base = CalendarAgg.joinCalendars(base0, g.aggs(), calCtx,
                spec -> scalar(spec, (v, name) -> resolveOrThrow(base0, name)),
                () -> aliasCounter++);
        List<OutputCol> contract = outputsOf(g.info());
        List<SqlExpr> keys = new ArrayList<>(g.keys().size());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (TypedGroupBy.GroupKey k : g.keys()) {
            SqlExpr e = k.fn().isPresent()
                    ? scalar(last(k.fn().get()), (v, name) -> resolveOrThrow(base, name))
                    : resolveOrThrow(base, k.column());
            // an enum-DECODE key groups on its RAW source (C1.4, engine
            // parity); the projection keeps the decoded name
            keys.add(com.legend.sql.DecodeShapes.sourceExpr(e).orElse(e));
            // a self-aliased key drops the alias (view-frame goldens) —
            // EXCEPT reads of a union frame's outputs, which keep it
            // ("unionalias_0"."lastName" as "lastName")
            boolean unionRead = base.from() instanceof SqlSource.Subselect sub
                    && "unionAlias".equals(sub.frameName())
                    && e instanceof SqlExpr.Column uc
                    && sub.alias().equals(uc.table());
            ps.add(new SqlSelect.Projection(e,
                    !unionRead && e instanceof SqlExpr.Column c
                            && c.name().equals(k.column()) ? null : k.column(),
                    Fold.named(contract, k.column())));
        }
        AggCols ac = aggCols(base, g.aggs(), calCtx, contract);
        ps.addAll(ac.ps());
        return ac.base().withGroupBy(keys)
                .withProjections(ps);
    }

    /** One envelope lambda lowered STRICTLY against the base select —
     * leaves, subType patches, witnesses, order keys and checked
     * constraints share the rule: read your own row only (an outer-scope
     * fallback could silently supply a same-named parent column, audit
     * L2); a miss is loud naming the site. */
    private SqlExpr envelopeScalar(TypedFuncCol cc, SqlSelect base,
            String what) {
        switch (attempt(() -> scalar(last(cc.fn()),
                (v, name) -> resolveOrThrow(base, name)))) {
            case Resolution.Resolved r -> {
                return r.expr();
            }
            case Resolution.Unfoldable u -> throw new IllegalStateException(
                    what + " '" + cc.name() + "' references column '"
                    + u.column() + "', unresolvable in the envelope source");
        }
    }

    /** THE JSON EGRESS CONFORM (§4bZ-V B4): serialized-graph values
     * are BUILT as database JSON — composition needs the real type
     * (nested aggregates feed parent json_object slots, so the value
     * must stay JSON while composing) — and become TEXT exactly once,
     * at the STATEMENT egress where the contract says String. A
     * synth-conformance cast: execution SQL spells the database's own
     * JSON&rarr;text serialization (probed byte-identical on 1.5.0,
     * empty case included), engine TEXT elides it (the conform()
     * suppression). Identity on non-JSON results. */
    private static SqlSelect conformJsonEgress(SqlSelect s) {
        if (s.projections().size() != 1) {
            return s;
        }
        SqlSelect.Projection p = s.projections().get(0);
        if (!(p.expr().type() instanceof com.legend.sql.TypeFact.Typed t)
                || t.type() != SqlType.Scalar.JSON) {
            return s;
        }
        return s.withProjections(
                List.of(new SqlSelect.Projection(new SqlExpr.Cast(
                        p.expr(), SqlType.Scalar.VARCHAR, true),
                        p.outputName(), p.out())));
    }

    private SqlSelect serializeGraph(TypedSerializeGraph g,
            boolean streamRoot) {
        SqlSelect src = relation(g.source());
        // json_group_array is an AGGREGATE and the envelope REPLACES the
        // projection list — the groupBy folding constraints are exactly right.
        SqlSelect base0 = Fold.groupByFolds(src) ? src : isolate(src);
        // §4AD batch-6 tail: fr[0] = the LIVE frame (decorrelated
        // reducer leaves add grouped LEFT joins to it)
        SqlSelect[] fr = {base0};
        ColumnResolver own = scopedResolver(base0, g.rowVar());
        List<SqlExpr> kv = new ArrayList<>(2 * (g.leaves().size() + g.nested().size()));
        java.util.Set<String> arrayProps = new java.util.LinkedHashSet<>();
        for (TypedFuncCol leaf : g.leaves()) {
            kv.add(new SqlExpr.StringLit(leaf.name()));
            kv.add(GraphAggDecorrelate.apply(fr, Fold.jsonDateWrap(
                    envelopeScalar(leaf, fr[0], "serialize leaf"),
                    Fold.leafResultType(leaf)), this::nextAlias));
        }
        for (var child : g.nested()) {
            kv.add(new SqlExpr.StringLit(child.property()));
            if (child.node().arrayWrap()) {
                arrayProps.add(child.property());
            }
            if (child.node().inlineChild()) {
                // EMBEDDED child: same-row json object, leaves resolve
                // against the PARENT select — no subquery (task #78 H4b)
                kv.add(inlineWrapped(fr[0], child.node()));
                continue;
            }
            enclosing.push(own);
            try {
                kv.add(new SqlExpr.ScalarSubquery(
                        serializeGraph(child.node(), false)));
            } finally {
                enclosing.pop();
            }
        }
        // includeType: (typeKey, concrete-type) leads each object
        List<SqlExpr> baseKv = kv;
        if (g.typeKeyName() != null && g.classFqn() != null && !g.bareValue()) {
            baseKv = new ArrayList<>();
            baseKv.add(new SqlExpr.StringLit(g.typeKeyName()));
            baseKv.add(new SqlExpr.StringLit(SnapshotEnvelope.typeName(g.classFqn(), g.fqTypePath())));
            baseKv.addAll(kv);
        }
        // bareValue: a to-many PRIMITIVE leaf aggregates raw values.
        // removeNull/removeEmpty: per-key json_object singletons folded
        // through json_merge_patch — RFC 7386 merge REMOVES null-valued
        // keys, which IS the engine's removePropertiesWithNullValues;
        // removeEmptySets maps a child's '[]' aggregate to NULL first.
        SqlExpr obj = g.bareValue() ? kv.get(1)
                : g.removeNullKeys() || g.removeEmptySets()
                        ? SnapshotEnvelope.mergePatchObject(baseKv,
                                arrayProps, g.removeEmptySets())
                        : new SqlExpr.JsonObject(baseKv);
        // ->subType views: DISJOINT members -> ONE CASE over the witnesses;
        // a member's branch serializes fr[0] + subtype fields IN FULL (engine
        // keeps "coordinate":null on member rows); non-members fall through
        if (!g.subTypePatches().isEmpty() && !g.bareValue()) {
            List<SqlExpr.Case.When> whens = new ArrayList<>();
            for (var p : g.subTypePatches()) {
                List<SqlExpr> pkv = new ArrayList<>();
                if (g.typeKeyName() != null) {
                    pkv.add(new SqlExpr.StringLit(g.typeKeyName()));
                    pkv.add(new SqlExpr.StringLit(
                            SnapshotEnvelope.typeName(p.subTypeFqn(), g.fqTypePath())));
                }
                pkv.addAll(kv);
                for (TypedFuncCol leaf : p.leaves()) {
                    pkv.add(new SqlExpr.StringLit(leaf.name()));
                    pkv.add(Fold.jsonDateWrap(
                            envelopeScalar(leaf, fr[0], "subType patch leaf"),
                            Fold.leafResultType(leaf)));
                }
                for (var child : p.children()) {
                    pkv.add(new SqlExpr.StringLit(child.property()));
                    enclosing.push(own);
                    try {
                        pkv.add(new SqlExpr.ScalarSubquery(
                                serializeGraph(child.node(), false)));
                    } finally {
                        enclosing.pop();
                    }
                }
                SqlExpr member = envelopeScalar(p.member(), fr[0],
                        "subType membership witness");
                whens.add(new SqlExpr.Case.When(member,
                        new SqlExpr.JsonObject(pkv)));
            }
            obj = new SqlExpr.Case(whens, obj);
        }
        // CHECKED envelope: {defects: [...], value: obj} — extracted rule
        if (g.checkedConstraints() != null) {
            obj = CheckedEnvelope.wrap(g, obj,
                    cc -> envelopeScalar(cc, fr[0], "checked constraint"));
        }
        if (g.objectRefPrefix() != null) {   // ASOR {objectReference, value}
            obj = SnapshotEnvelope.asorWrap(g, obj,
                    k -> envelopeScalar(k, fr[0], "objectReference pk"));
        }
        SqlSelect envelope = fr[0];
        SqlExpr result;
        if (g.arrayWrap()) {
            List<SqlExpr.JsonArrayAgg.Key> okeys =
                    new ArrayList<>(g.orderKeys().size());
            for (TypedFuncCol k : g.orderKeys()) {
                if (!k.name().startsWith(TypedSerializeGraph.PK_ORDER_PREFIX)) {
                    // union WITNESS key: DESC (TRUE-first), load-bearing
                    okeys.add(new SqlExpr.JsonArrayAgg.Key(
                            envelopeScalar(k, fr[0], "envelope order key"), true));
                    continue;
                }
                // PK determinism key: ASC and BEST-EFFORT — an explicit
                // fr[0] projection resolves normally; a star pass-through
                // resolves against the DRIVING (leftmost) source only (pk
                // spellings collide across tables — never bind a guessed
                // join side). Pruned or ambiguous columns skip, scan order
                // stands.
                String col = k.name().substring(
                        TypedSerializeGraph.PK_ORDER_PREFIX.length());
                boolean explicit = fr[0].projections().stream()
                        .anyMatch(p -> col.equals(p.outputName()));
                SqlExpr pkE = null;
                if (explicit) {
                    if (attempt(() -> scalar(last(k.fn()),
                            (v, name) -> resolveOrThrow(fr[0], name)))
                            instanceof Resolution.Resolved r) {
                        pkE = r.expr();
                    }
                } else if (fr[0].projections().isEmpty()
                        || fr[0].projections().stream().anyMatch(
                                p -> p.expr() instanceof SqlExpr.Star)) {
                    pkE = Fold.sourceColumnDriving(fr[0].from(), col);
                }
                if (pkE instanceof SqlExpr.Column pc
                        && !Fold.physicallyRenderable(fr[0].from(), pc)) {
                    pkE = null;   // stale stamping — skip, scan order stands
                }
                if (pkE != null) {
                    okeys.add(new SqlExpr.JsonArrayAgg.Key(pkE, false));
                }
            }
            // UNION-MEMBER serial order (engine contract: union members
            // serialize in BRANCH DECLARATION order — the engine's stitch
            // is serial per member; json_group_array inherits scan order):
            // a UNION ALL under pass-through selects gains a per-branch
            // ordinal projection, NEGATED because the ordered-agg renders
            // DESC (the witness TRUE-first contract). SQL-level only —
            // the typed schema never sees the column.
            SqlSelect withOrd = UnionSerialOrder.inject(fr[0]);
            if (withOrd != null) {
                envelope = withOrd;
                okeys.add(0, new SqlExpr.JsonArrayAgg.Key(
                        resolveOrThrow(envelope, UnionSerialOrder.COLUMN), true));
            }
            if (streamRoot) {
                return StreamingGraphRoot.select(envelope, obj, okeys);
            }
            result = new SqlExpr.JsonArrayAgg(obj, okeys);
        } else {
            result = obj;
        }
        return envelope.withProjections(
                List.of(new SqlSelect.Projection(result, "result",
                        new OutputCol("result",
                                PureSql.type(Type.Primitive.STRING),
                                false))));
    }

    /** An INLINE (embedded) child's json object over the parent select:
     * leaves resolve strictly against the SAME base; inline children
     * recurse; correlated-inside-embedded keeps the subquery. */
    /** An inline child, ARRAY-wrapped when the property is to-many
     * (engine: "authors":[{...}] — the embedded instance rides in a
     * one-element JSON array). */
    private SqlExpr inlineWrapped(SqlSelect base, TypedSerializeGraph g) {
        SqlExpr obj = inlineChildObject(base, g);
        return g.arrayWrap()
                ? SqlExpr.Call.of(SqlFn.TO_VARIANT,
                        new SqlExpr.ArrayLit(List.of(obj)))
                : obj;
    }

    private SqlExpr inlineChildObject(SqlSelect base, TypedSerializeGraph g) {
        List<SqlExpr> kv = new ArrayList<>(
                2 * (g.leaves().size() + g.nested().size()));
        for (TypedFuncCol leaf : g.leaves()) {
            kv.add(new SqlExpr.StringLit(leaf.name()));
            switch (attempt(() -> scalar(last(leaf.fn()),
                    (v, name) -> resolveOrThrow(base, name)))) {
                case Resolution.Resolved r -> kv.add(Fold.jsonDateWrap(
                        r.expr(), Fold.leafResultType(leaf)));
                case Resolution.Unfoldable u -> throw new IllegalStateException(
                        "embedded serialize leaf '" + leaf.name()
                                + "' references column '" + u.column()
                                + "', unresolvable in the parent envelope");
            }
        }
        for (var child : g.nested()) {
            kv.add(new SqlExpr.StringLit(child.property()));
            if (child.node().inlineChild()) {
                kv.add(inlineWrapped(base, child.node()));
            } else {
                enclosing.push(scopedResolver(base, g.rowVar()));
                try {
                    kv.add(new SqlExpr.ScalarSubquery(
                            serializeGraph(child.node(), false)));
                } finally {
                    enclosing.pop();
                }
            }
        }
        return new SqlExpr.JsonObject(kv);
    }

    /** aggregate: whole-relation reduction — aggregates only, no GROUP BY clause. */
    private SqlSelect aggregate(TypedAggregate a) {
        SqlSelect src = relation(a.source());
        SqlSelect base = Fold.groupByFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "aggregate", b -> {
            AggCols ac = aggCols(b, a.aggs(), null, outputsOf(a.info()));
            return ac.base().withProjections(ac.ps());
        });
    }

    /** Aggregate projections with the union-order obligation applied
     * (Fold.orderUnionAggregate may rebuild the base's union source). */
    private record AggCols(SqlSelect base, List<SqlSelect.Projection> ps) {
    }

    private AggCols aggCols(SqlSelect base, List<TypedAggCol> aggs,
            java.util.@com.legend.Nullable Map<TypedAggCol, CalendarAgg.Ctx> cal,
            List<OutputCol> contract) {
        List<SqlSelect.Projection> ps = new ArrayList<>(aggs.size());
        for (TypedAggCol a : aggs) {
            SqlExpr av = aggValue(base, a, cal == null ? null : cal.get(a));
            // §3b: the obligation sees through wrapping Calls
            if (Fold.orderUnionAggregateExpr(base, av)
                    instanceof Fold.OrderedAggExpr oa) {
                base = oa.base();
                av = oa.expr();
            }
            ps.add(new SqlSelect.Projection(av, a.name(),
                    Fold.named(contract, a.name())));
        }
        return new AggCols(base, ps);
    }

    /**
     * One agg column: the map lambda yields the value expression; the reduce
     * lambda's resolved overload names the SQL reducer. A bare-row map
     * ({@code x|$x}) is COUNT(*)-style — no value argument.
     */
    SqlAgg.Reducer aggExpr(SqlSelect base, TypedAggCol a) {
        SqlExpr e = aggValue(base, a);
        if (!(e instanceof SqlAgg.Reducer r)) {
            throw new IllegalStateException("aggregate '" + a.name()
                    + "' composes multiple reducers (wavg) — PIVOT USING"
                    + " takes exactly one");
        }
        return r;
    }

    private SqlExpr aggValue(SqlSelect base, TypedAggCol a) {
        return aggValue(base, a, null);
    }

    /** The aggregate's per-row selector body. A singleton COLLECTION-LITERAL
     * selector (agg(x|[$x.emps.age], y|$y->sum())) unwraps: Pure collection
     * literals flatten — the per-row aggregated value IS the element ([xs]
     * carries no nesting). The wrapper exists for overload dispatch
     * (collection reductions vs to-one identity, the singletonListReductions
     * pin), and the reducer overload is already resolved here — SUM(age),
     * not SUM([age]). */
    private static TypedSpec aggSelectorBody(TypedAggCol a) {
        TypedSpec mapBody = last(a.map());
        while (mapBody instanceof TypedCollection stc
                && stc.elements().size() == 1) {
            mapBody = stc.elements().get(0);
        }
        return mapBody;
    }

    private SqlExpr aggValue(SqlSelect base, TypedAggCol a,
            CalendarAgg.@com.legend.Nullable Ctx calendar) {
        TypedSpec reduceBody = last(a.reduce());
        // A cast WRAPPING the reducer (y|$y->plus()->cast(@Integer)) rides
        // AROUND the SQL aggregate: unwrap, lower the inner reducer, re-wrap
        // by the cast policy (widening/same-type is the assertion no-op —
        // the PCT shapes are Integer->Integer).
        if (reduceBody instanceof TypedCast rc
                && rc.source() instanceof TypedNativeCall) {
            SqlExpr inner = aggValue(base, new TypedAggCol(a.name(), a.map(),
                    new TypedLambda(a.reduce().parameters(),
                            List.of(rc.source()), a.reduce().info()),
                    a.orderKey(), a.orderAsc()));
            return CastPolicy.castByPolicy(inner, rc.source().info().type(), rc.target(), rc.wire());
        }
        if (!(reduceBody instanceof TypedNativeCall call)) {
            throw new IllegalStateException("aggregate reduce must be a native reducer call, got "
                    + reduceBody.getClass().getSimpleName());
        }
        // A SCALAR around the reducer — y|$y->average()->round(), or the
        // reducer inside an arithmetic run (y|$y->sum() * 2 = times([sum(y), 2]))
        SqlExpr around = aroundReducer(base, a, call);
        if (around != null) {
            return around;
        }
        SqlAgg.Fn fn = Aggregates.reducerFor(call.callee());
        TypedSpec mapBody = aggSelectorBody(a);
        // ORDER-SENSITIVE aggregation (sortBy before joinStrings): the key
        // lowers in the SAME row scope as the map body and rides inside
        // the SQL aggregate (string_agg(x, sep ORDER BY k))
        List<SqlSelect.SortKey> aggOrder = a.orderKey() == null ? List.of()
                : List.of(new SqlSelect.SortKey(
                        scalar(last(a.orderKey()),
                                (v, name) -> resolveOrThrow(base, name)),
                        a.orderAsc(), null, null));
        // Reducer EXTRA arguments (joinStrings('_') carries its separator;
        // percentile carries p [+ ascending, continuous]): literal args ride
        // along after the value; variable refs are the reducer's own
        // collection params; anything ELSE is unsupported and must be LOUD.
        List<SqlExpr> extra = new ArrayList<>();
        List<Boolean> flags = new ArrayList<>();
        TypedCast valueCast = null;
        boolean distinctValues = false;
        boolean descending = false;
        for (TypedSpec argSpec : call.args()) {
            if (argSpec instanceof TypedNativeCall dn
                    && dn.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::distinct")
                    && dn.args().size() == 1
                    && dn.args().get(0) instanceof TypedVariable) {
                // y|$y->distinct()->count(): DISTINCT inside the SQL
                // aggregate (engine: count(distinct(col)))
                distinctValues = true;
            } else if (argSpec instanceof TypedCBoolean b) {
                flags.add(b.value());
            } else if (argSpec instanceof TypedCString
                    || argSpec instanceof TypedCInteger
                    || argSpec instanceof TypedCFloat
                    || argSpec instanceof TypedCDecimal
                    // NEGATED numeric literal (uniqueValueOnly(-1)): pure
                    // spells -1 as minus(1) — same literal channel
                    || argSpec instanceof TypedNativeCall neg
                            && neg.args().size() == 1
                            && com.legend.builtin.Pure.nativeNamed("minus",
                                    neg.callee().signatureKey())
                            && (neg.args().get(0) instanceof TypedCInteger
                                    || neg.args().get(0) instanceof TypedCFloat
                                    || neg.args().get(0) instanceof TypedCDecimal)) {
                extra.add(scalar(argSpec, (v, name) -> resolveOrThrow(base, name)));
            } else if (argSpec instanceof TypedCast vc
                    && vc.source() instanceof TypedVariable) {
                // $x->cast(@T)->plus(): the grouped VALUES cast before
                // reducing — a value-cast on the aggregated expression.
                valueCast = vc;
            } else if (!(argSpec instanceof TypedVariable)) {
                throw new IllegalStateException("aggregate reducer argument of kind "
                        + argSpec.getClass().getSimpleName()
                        + " is not supported (literals only)");
            }
        }
        Aggregates.AggFlavor flavor = Aggregates.aggFlavor(fn, flags, extra.size());
        fn = flavor.fn();
        descending = flavor.descending();
        // BI-VARIATE map: rowMapper(value, key) decomposes into the SQL
        // aggregate's two arguments — CORR(a, b), ARG_MAX(v, k), ...
        if (mapBody instanceof TypedNativeCall rm
                && com.legend.builtin.NativeFn.LowererForm.isBivariateMap(rm.callee().qualifiedName())
                && rm.args().size() == 2) {
            if (descending) {
                // this arm returns without the within-group order —
                // reaching it with the flag set would silently drop DESC
                throw new IllegalStateException(
                        "descending percentile over a rowMapper body"
                        + " has no lowering");
            }
            SqlExpr first = scalar(rm.args().get(0), (v, name) -> resolveOrThrow(base, name));
            SqlExpr second = scalar(rm.args().get(1), (v, name) -> resolveOrThrow(base, name));
            if (fn == SqlAgg.Fn.WAVG) {
                // Weighted average: SUM(v*w)/SUM(w) — no single SQL reducer.
                return SqlExpr.Call.of(SqlFn.DIVIDE,
                        new SqlAgg.Reducer(SqlAgg.Fn.SUM,
                                List.of(SqlExpr.Call.of(SqlFn.TIMES, first, second)), false, java.util.List.of()),
                        new SqlAgg.Reducer(SqlAgg.Fn.SUM, List.of(second), false, java.util.List.of()));
            }
            return new SqlAgg.Reducer(fn, List.of(first, second), false, java.util.List.of());
        }
        if (fn == SqlAgg.Fn.WAVG) {
            throw new IllegalStateException(
                    "wavg expects a rowMapper(value, weight) map body");
        }
        if ((distinctValues || fn == SqlAgg.Fn.IS_DISTINCT_MARK)
                && mapBody instanceof TypedVariable) {
            throw new com.legend.error.NotImplementedException(
                    "DISTINCT aggregate over a bare group variable — no value"
                    + " column to deduplicate");
        }
        if (mapBody instanceof TypedVariable && extra.isEmpty()) {
            // MARKER reducers never take the bare COUNT(*)-style return —
            // the marker would escape its contract as literal SQL
            // (audit 23 C-a: uniqueValueOnly/hashCode over a bare row)
            if (fn.marker()) {
                throw new com.legend.error.NotImplementedException(
                        "composed aggregate '" + call.callee().qualifiedName()
                        + "' over a bare group variable — no value column");
            }
            return new SqlAgg.Reducer(fn, List.of(), false, java.util.List.of());
        }
        // count-of-rows desugar (x|$x -> x|1): count(*), count(1) on UNIONs
        if (fn == SqlAgg.Fn.COUNT && mapBody instanceof TypedCInteger one
                && one.value().longValue() == 1 && extra.isEmpty()
                && !distinctValues && valueCast == null
                && !Fold.unionBacked(base.from())) {
            return new SqlAgg.Reducer(fn, List.of(), false, java.util.List.of());
        }
        // CALENDAR native in map position: the value is the CASE over the
        // pre-joined calendar aliases; the fn's VALUE argument aggregates
        if (calendar != null
                && CalendarAgg.calendarCallOf(mapBody)
                        instanceof TypedNativeCall calCall) {
            SqlExpr calVal = scalar(calCall.args().get(3),
                    (v, name) -> resolveOrThrow(base, name));
            return new SqlAgg.Reducer(fn,
                    List.of(CalendarAgg.caseValue(calCall, calendar, calVal)),
                    false, java.util.List.of());
        }
        SqlExpr value = scalar(mapBody, (v, name) -> resolveOrThrow(base, name));
        if (valueCast != null) {
            value = CastPolicy.castByPolicy(value,
                    valueCast.source().info().type(), valueCast.target(), valueCast.wire());
        }
        // isDistinct over a group: COUNT(DISTINCT x) = COUNT(x) — no single
        // SQL reducer (engine testGroupByIsDistinct golden).
        if (fn == SqlAgg.Fn.IS_DISTINCT_MARK) {
            if (!extra.isEmpty()) {
                throw new IllegalStateException("isDistinct aggregate with"
                        + " extra arguments — the group form takes none"
                        + " (audit 22a M5: dropped extras rendered the group"
                        + " SQL for a non-group call)");
            }
            return SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(value), true, java.util.List.of()),
                    new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(value), false, java.util.List.of()));
        }
        if (fn == SqlAgg.Fn.UNIQUE_VALUE_ONLY) {
            return uniqueValueOnlyAgg(extra, value);
        }
        // hashCode over a group: HASH(LIST(values)) — no single SQL
        // reducer. Signed-64 conformance (pure hashCode is Integer[1])
        // is owned by the DIALECT hashSigned arm — one owner, never a
        // second shift here.
        if (fn == SqlAgg.Fn.HASH_LIST) {
            return SqlExpr.Call.of(SqlFn.HASH,
                    new SqlAgg.Reducer(SqlAgg.Fn.LIST, List.of(value), false, java.util.List.of()));
        }
        // 1-arg joinStrings joins with the EMPTY separator (string-
        // Extension.pure:253) — bare STRING_AGG defaults to COMMA.
        if (fn == SqlAgg.Fn.STRING_AGG && extra.isEmpty()) {
            extra.add(new SqlExpr.StringLit(""));
        }
        // ORDER DETERMINISM: an un-ordered group concat follows SCAN
        // order on the engine's H2 (insertion order — Johnson*Hill,
        // S1*S2 goldens); DuckDB's hash joins scramble it. The faithful
        // key is the VALUE table's physical row order — rowid, valid
        // only when the value reads a BASE TABLE alias.
        if (fn == SqlAgg.Fn.STRING_AGG && aggOrder.isEmpty()
                && value instanceof SqlExpr.Column vc
                && aliasIsBaseTable(base.from(), vc.table())) {
            aggOrder = List.of(new SqlSelect.SortKey(
                    new SqlExpr.RowOrder(vc.table()), true, null, null));
        }
        // (Sorted-input aggregation order is ENGINE-COMPAT ONLY —
        // StableScanOrder owns replay determinism; user ruling
        // 2026-08-31: the platform stays order-honest.)
        // joinStrings(prefix, sep, suffix): STRING_AGG takes only the
        // separator — prefix/suffix concatenate AROUND the aggregate.
        if (fn == SqlAgg.Fn.STRING_AGG && extra.size() == 3) {
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, extra.get(0),
                            new SqlAgg.Reducer(fn, List.of(value, extra.get(1)),
                                    false, aggOrder)),
                    extra.get(2));
        }
        // a DESCENDING percentile is the SAME reducer over the value's
        // descending within-group order (SQL-standard PERCENTILE_x(p)
        // WITHIN GROUP (ORDER BY v DESC)); each dialect spells it —
        // DuckDB's quantile family takes no order, so its renderer owns
        // the negation / sorted-list encodings
        if (descending) {
            aggOrder = List.of(new SqlSelect.SortKey(value, false, null, null));
        }
        List<SqlExpr> args = new ArrayList<>();
        args.add(value);
        args.addAll(extra);
        SqlExpr red = new SqlAgg.Reducer(fn, args, distinctValues, aggOrder);
        // pure percentile RENDERS AS FLOAT (engine golden 12.0, not 12) —
        // the discrete quantile keeps the input's integer type, so cast
        return fn == SqlAgg.Fn.QUANTILE_DISC
                ? new SqlExpr.Cast(red, SqlType.Scalar.DOUBLE) : red;
    }

    /**
     * extend (append=true) / project (append=false) with computed columns.
     * Column lambdas resolve against the CURRENT select via substitution, so
     * a plain-projection or star select stays flat.
     */
    /** extend(~cols): existing projections stay, computed columns APPEND. */
    private SqlSelect extend(SqlSelect src, List<TypedFuncCol> columns,
                             ExprType info) {
        SqlSelect base = Fold.extendFolds(src) ? src : isolate(src);
        return computedColumns(base, columns, info, true, false);
    }

    /** project(~cols): the computed columns REPLACE the projection list.
     * {@code wireForm} — the flat class form's engine-wire provenance
     * (TypedProject.wireForm): conformance casts are suppressed there. */
    private SqlSelect project(SqlSelect src, List<TypedFuncCol> columns,
                              ExprType info, boolean wireForm) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        return computedColumns(base, columns, info, false, wireForm);
    }

    /** Lower computed columns over {@code base}: one attempt, isolate ONCE on
     * an unfoldable ref, then loud (isolation is idempotent for resolution). */
    private SqlSelect computedColumns(SqlSelect base, List<TypedFuncCol> columns,
                                      ExprType info,
                                      boolean keepExisting,
                                      boolean wireForm) {
        String[] miss = new String[2];
        SqlSelect a1 = tryComputedColumns(base, columns, info, keepExisting,
                wireForm, miss);
        SqlSelect a2 = a1 != null ? a1 : tryComputedColumns(isolate(base),
                columns, info, keepExisting, wireForm, miss);
        if (a2 != null) {
            return a2;
        }
        throw new IllegalStateException("extend/project columns "
                + columns.stream().map(TypedFuncCol::name).toList()
                + " reference names unresolvable even after isolation"
                + (miss[0] == null ? "" : " [col='" + miss[0] + "' ref='" + miss[1] + "']")
                + " over " + base.projections().stream().map(SqlSelect.Projection::alias).toList());
    }

    /** One pass; null when any column's refs would not fold against {@code base}. */
    private @com.legend.Nullable SqlSelect tryComputedColumns(SqlSelect base, List<TypedFuncCol> columns,
                                         ExprType info,
                                         boolean keepExisting,
                                         boolean wireForm, String[] miss) {
        // E2 (JAVA_EVICTION_PLAN): a TO-MANY scalar funcCol EXPLODES ROWS
        // IN SQL — the engine's scalar-stream rule (one row per element,
        // row-major; an EMPTY stream keeps one parent row with a NULL
        // cell — the LEFT LATERAL). The declared TDS column is to-one and
        // the emitted slot matches it; the Executor's host-side explosion
        // (audit A13) is dead. ONE such column per project (engine wall).
        List<TypedFuncCol> manyCols = columns.stream()
                .filter(Fold::isManyScalarCol).toList();
        if (manyCols.size() > 1) {
            throw new com.legend.error.NotImplementedException(
                    "two many-valued TDS columns in one project ('"
                    + manyCols.get(0).name() + "', '"
                    + manyCols.get(1).name()
                    + "') — only single-column row explosion is built");
        }
        if (!manyCols.isEmpty()
                && (!base.groupBy().isEmpty() || base.distinct())) {
            base = isolate(base);
        }
        SqlSelect target = base;
        List<OutputCol> contract = outputsOf(info);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (keepExisting) {
            // starProjections handles the JOIN-source case (no single alias:
            // bare * over the disjoint-by-invariant sides).
            ps.addAll(starProjections(base));
        }
        for (TypedFuncCol c : columns) {
            TypedSpec cellRoot = last(c.fn());
            TypedSpec body = CastPolicy.cellRootUnwrapWire(cellRoot);
            // a STRIPPED wire cast leaves the bare mismatched read —
            // it takes the same engine-compat tag the typeAsDeclared
            // door applies (§4bZ): the unwrap site is the other place
            // that KNOWS the read crossed a declared mismatch
            boolean unwrapped = body != cellRoot;
            SqlSelect resolveBase = base;
            switch (attempt(() -> {
                SqlExpr lowered = scalar(body,
                        (v, name) -> resolveOrThrow(resolveBase, name));
                return unwrapped
                        ? com.legend.sql.SqlTyping.tolerateRead(lowered)
                        : lowered;
            })) {
                case Resolution.Resolved r -> {
                    if (manyCols.contains(c)) {
                        Type elemT = Type.schemaView(info.type()) instanceof Type.RelationType rt
                                ? rt.columns().stream()
                                        .filter(cc -> cc.name().equals(c.name()))
                                        .findFirst().map(Type.Column::type)
                                        .orElse(Type.Primitive.STRING)
                                : Type.Primitive.STRING;
                        String lat = nextAlias();
                        target = target.withFrom(new SqlSource.Join(
                                target.from(),
                                Fold.lateralElem(r.expr(),
                                        PureSql.type(elemT),
                                        nextAlias(), lat),
                                SqlSource.Join.Kind.LEFT_LATERAL,
                                new SqlExpr.BoolLit(true)));
                        ps.add(new SqlSelect.Projection(
                                // §E3: LEFT_LATERAL pads with NULL on
                                // an empty array — nullable slot
                                SqlExpr.Column.of(lat, "elem",
                                        PureSql.type(elemT), true,
                                        com.legend.sql.OutputCol.Origin.DERIVED),
                                c.name(), Fold.named(contract, c.name())));
                        continue;
                    }
                    ps.add(new SqlSelect.Projection(r.expr(), c.name(),
                            Fold.named(contract, c.name())));
                }
                case Resolution.Unfoldable u -> {
                    miss[0] = c.name();
                    miss[1] = u.column();   // the caller reports the miss
                    return null;
                }
            }
        }
        return target.withProjections(ps);
    }

    private SqlSelect filter(TypedFilter f) {
        SqlSelect src = relation(f.source());
        boolean windowRef = false;
        SqlExpr predicate = null;
        // the PREDICATE lowers in filter position (NullSemantics
        // null-safe equal arm — engine callingFromFilter); the SOURCE
        // above lowered OUTSIDE the boundary; a CORRELATION filter = mapping join: VERBATIM '='
        boolean prevVerbatim = verbatimEquality;
        verbatimEquality = verbatimEquality || f.stamp() == TypedFilter.Stamp.CORRELATION;
        try {
        if (tryPredicate(src, f.predicate()) instanceof Resolution.Resolved r) {
            predicate = r.expr();
        } else if (src.groupBy().isEmpty()) {
            // Window-aware path: refs to window-column aliases substitute the
            // WindowCall itself — QUALIFY admits window expressions.
            WindowPredicate viaProjections = tryWindowPredicate(src, f.predicate());
            if (viaProjections != null && viaProjections.sawWindow()) {
                predicate = viaProjections.expr();
                windowRef = true;
            }
        }
        if (predicate == null) {
            src = isolate(src);
            predicate = predicateOrThrow(src, f.predicate(), "filter");
        }
        } finally {
            verbatimEquality = prevVerbatim;
        }
        Fold.FilterSlot slot = Fold.filterSlot(src, windowRef);
        if (slot == Fold.FilterSlot.ISOLATE) {
            src = isolate(src);
            predicate = predicateOrThrow(src, f.predicate(), "filter");
            slot = Fold.filterSlot(src, false);
        }
        return switch (slot) {
            case WHERE -> {
                SqlSelect m = src.withWhere(
                        mergeWhere(src.where(), predicate, f.stamp()));
                yield engineExistsJoinForm
                        ? ExistsJoinForm.rewrite(m, this::nextAlias,
                                whereZones::get)
                        : m;
            }
            case HAVING -> src.withHaving(src.having() == null ? predicate
                    : Fold.mergeAnd(src.having(), predicate));
            case QUALIFY -> src.withQualify(src.qualify() == null ? predicate
                    : Fold.mergeAnd(src.qualify(), predicate));
            case ISOLATE -> throw new IllegalStateException("unreachable: isolated above");
        };
    }

    /** The scalar-around-the-reducer shape: the ONE reducer among the
     *  operands (direct, or inside an arithmetic run) lowers as the
     *  aggregate, the rest as literals (no row scope), the scalar's rule
     *  applies around it; null for any other shape. */
    private @com.legend.Nullable SqlExpr aroundReducer(SqlSelect base, TypedAggCol a,
            TypedNativeCall call) {
        if (Aggregates.reducerOrNull(call.callee()) != null || call.args().isEmpty()) {
            return null;
        }
        List<TypedNativeCall> reducers = new ArrayList<>();
        for (TypedSpec arg : call.args()) {
            for (TypedSpec e : arg instanceof TypedCollection run ? run.elements() : List.of(arg)) {
                if (e instanceof TypedNativeCall rc && Aggregates.reducerOrNull(rc.callee()) != null) {
                    reducers.add(rc);
                }
            }
        }
        if (reducers.size() != 1) {
            return null;
        }
        TypedNativeCall reducer = reducers.get(0);
        SqlExpr inner = aggValue(base, new TypedAggCol(a.name(), a.map(),
                new TypedLambda(a.reduce().parameters(),
                        List.of(reducer), a.reduce().info()),
                a.orderKey(), a.orderAsc()));
        java.util.function.Function<TypedSpec, SqlExpr> operand =
                e -> e == reducer ? inner : scalar(e, noScope());
        List<SqlExpr> wrapped = new ArrayList<>();
        for (TypedSpec arg : call.args()) {
            wrapped.add(arg instanceof TypedCollection run ? listLiteral(run, operand) : operand.apply(arg));
        }
        return NullSemantics.verbatim(verbatimEquality || legacyNullUnsafeEquals, Scalars.lower(call, wrapped, features));
    }

    /** The list literal a collection lowers to — ONE construction site;
     *  the element lowering is the caller's channel (plain or window). */
    static SqlExpr listLiteral(TypedCollection c,
            java.util.function.Function<TypedSpec, SqlExpr> element) {
        return new SqlExpr.ArrayLit(c.elements().stream().map(element).toList());
    }

    /** whereExpr identity -> its {@link WhereMerge.Zones} split — written
     * whenever a resolver-stamped filter folds, consumed as later filters
     * fold onto the same WHERE (see {@link WhereMerge}). */
    private final java.util.IdentityHashMap<SqlExpr, WhereMerge.Zones>
            whereZones = new java.util.IdentityHashMap<>();

    private SqlExpr mergeWhere(@com.legend.Nullable SqlExpr existing,
            SqlExpr predicate,
            com.legend.compiler.spec.typed.TypedFilter.Stamp stamp) {
        return WhereMerge.merge(whereZones, existing, predicate, stamp);
    }

    /** The isolate-terminal boundary: the select was JUST isolated, so an
     * unfoldable ref can never become foldable — LOUD, never a dropped
     * predicate (the ONE retry contract, shared by filter and whereLambda). */
    private SqlExpr predicateOrThrow(SqlSelect isolated, TypedLambda lambda, String op) {
        return switch (tryPredicate(isolated, lambda)) {
            case Resolution.Resolved r -> r.expr();
            case Resolution.Unfoldable u -> throw new IllegalStateException(
                    op + " predicate references column '" + u.column()
                            + "', unresolvable even after isolation [param="
                            + lambda.parameters().get(0) + "; pred="
                            + lambda.body().get(lambda.body().size() - 1) + "]");
        };
    }

    private Resolution tryPredicate(SqlSelect select, TypedLambda lambda) {
        ColumnResolver columns = select.groupBy().isEmpty()
                ? scopedResolver(select, lambda.parameters().get(0))
                : (v, name) -> projectionExprOrThrow(select, name);
        return attempt(() -> scalar(last(lambda), columns));
    }

    private record WindowPredicate(SqlExpr expr, boolean sawWindow) {
    }

    /** Resolve refs via projections, noting whether any substituted a window call. */
    private @com.legend.Nullable WindowPredicate tryWindowPredicate(SqlSelect select, TypedLambda lambda) {
        var saw = new AtomicBoolean();
        return switch (attempt(() -> scalar(last(lambda), (v, name) -> {
            SqlExpr resolved = projectionExprOrThrow(select, name);
            if (resolved instanceof SqlExpr.WindowCall) {
                saw.set(true);
            }
            return resolved;
        }))) {
            case Resolution.Resolved r -> new WindowPredicate(r.expr(), saw.get());
            case Resolution.Unfoldable u -> null;
        };
    }

    /** A post-aggregation ref: the projection's expression, computed or not. */
    private SqlExpr projectionExprOrThrow(SqlSelect select, @com.legend.Nullable String column) {
        if (column == null) {
            // a bare-variable read has no projection to substitute
            throw new UnfoldableRef("<whole variable>");
        }
        for (SqlSelect.Projection p : select.projections()) {
            if (column.equals(p.outputName())) {
                return p.expr();
            }
        }
        throw new UnfoldableRef(column);
    }

    /** uniqueValueOnly over a group (collectionExtension.pure): the
     * single distinct value, else empty — CASE WHEN COUNT(DISTINCT x)
     * = 1 THEN MAX(x) END (max of one value IS the value); the 2-arg
     * form's DEFAULT rides as the CASE else. */
    private static SqlExpr uniqueValueOnlyAgg(List<SqlExpr> extra,
            SqlExpr value) {
        SqlExpr uvDefault = extra.isEmpty() ? new SqlExpr.NullLit()
                : extra.get(0);
        if (extra.size() > 1) {
            throw new IllegalStateException(
                    "uniqueValueOnly aggregate with " + extra.size()
                            + " extra arguments");
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL,
                        new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(value), true, java.util.List.of()),
                        new SqlExpr.IntLit(1)),
                new SqlAgg.Reducer(SqlAgg.Fn.MAX, List.of(value), false, java.util.List.of()))),
                uvDefault);
    }

    /** Whether {@code alias} names a BASE TABLE scan in the from tree —
     * the rowid pseudo-column is only valid there. */
    private static boolean aliasIsBaseTable(SqlSource src, @com.legend.Nullable String alias) {
        return switch (src) {
            case SqlSource.Table t -> t.alias().equals(alias);
            case SqlSource.Join j -> aliasIsBaseTable(j.left(), alias)
                    || aliasIsBaseTable(j.right(), alias);
            default -> false;
        };
    }

    SqlExpr resolveOrThrow(SqlSelect select, @com.legend.Nullable String column) {
        // bare-variable read ($var whole): over a SINGLE-COLUMN select the
        // row IS the cell (the encoding's value semantics); wider rows
        // stay unfoldable (isolate-or-loud), never NPE
        if (column == null) {
            column = select.projections().size() == 1
                    ? select.projections().get(0).outputName() : null;
            if (column == null) { throw new UnfoldableRef("<whole variable>"); }
        }
        SqlExpr resolved = Fold.resolveInto(select, column);
        if (resolved == null) {
            throw new UnfoldableRef(column);
        }
        return resolved;
    }

    /** Lambda-body resolver over {@code select} — the correlation channel.
     * VAR-AWARE: the lambda's OWN param resolves against the own select;
     * OTHER vars try ENCLOSING scopes FIRST (own-select-first silently
     * self-correlates on same-named columns — audit's two wrong-answer
     * regressions); non-own vars missing every scope are UNFOLDABLE. */
    private ColumnResolver scopedResolver(SqlSelect select, String ownVar) {
        // SNAPSHOT the enclosing scopes at creation: iterating the LIVE
        // deque includes this resolver itself once inner scopes run — any
        // correlated miss then recurses forever (audit: StackOverflow where
        // the contract promises a loud UnfoldableRef).
        var outers = List.copyOf(enclosing);
        return (var, name) -> {
            boolean own = var == null || var.equals(ownVar);
            if (own && attempt(() -> resolveOrThrow(select, name))
                    instanceof Resolution.Resolved o) {
                return o.expr();
            }
            for (var outer : outers) {
                if (attempt(() -> outer.resolve(var, name))
                        instanceof Resolution.Resolved found) {
                    return found.expr();
                }
            }
            // audit 23 C-c: NO own-select last resort for non-own vars —
            // the silent self-correlation resolved an outer row's read
            // against a same-named INNER column (the two regressions in
            // this method's doc-comment). A genuine miss takes the
            // designed UnfoldableRef route (isolate-or-loud).
            throw new UnfoldableRef(name == null ? "<whole variable>" : name);
        };
    }

    /**
     * Row-scope resolver: variable/property references to SQL expressions.
     * {@code propOrNull == null} means a bare {@code $var} reference. May
     * throw {@link UnfoldableRef}; callers at TRY boundaries convert via
     * {@link #attempt} (the ONE catch site).
     */
    /**
     * The RELATION-LEVEL try boundary: build the op over the folded select;
     * a computed-column reference (extend'ed expression, window column) is
     * unfoldable in place, so ISOLATE and rebuild — references then resolve
     * to plain columns of the subselect. Loud when isolation cannot cure it.
     */
    private SqlSelect foldOrIsolate(SqlSelect base, String op,
            Function<SqlSelect, SqlSelect> build) {
        try {
            return build.apply(base);
        } catch (UnfoldableRef first) {
            try {
                return build.apply(isolate(base));
            } catch (UnfoldableRef second) {
                throw new IllegalStateException(op + " reference '"
                        + second.getMessage() + "' cannot be resolved even after"
                        + " isolation");
            }
        }
    }

    /** select(~cols): narrow the projection list. */
    private SqlSelect narrowTo(SqlSelect src, List<String> columns,
                               ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        return projectColumns(base, columns, info);
    }

    /** distinct(~cols): narrow AND dedup (distinct has its own fold policy). */
    private SqlSelect distinctNarrowTo(SqlSelect src, List<String> columns,
                                       ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        if (!Fold.distinctNarrowFolds(base, columns)) {
            base = isolate(base);
        }
        return projectColumns(base, columns, info).withDistinct();
    }

    /**
     * Project {@code columns} off {@code base}, isolating ONCE if any fails
     * to resolve (then loud — isolation is idempotent for resolution). Two
     * clean attempts, no index-reset restarts.
     */
    private SqlSelect projectColumns(SqlSelect base, List<String> columns,
                                     ExprType info) {
        List<OutputCol> contract = outputsOf(info);
        List<SqlSelect.Projection> ps = Fold.tryProjectAll(base, columns,
                contract);
        if (ps == null) {
            base = isolate(base);
            ps = Fold.tryProjectAll(base, columns, contract);
            if (ps == null) {
                throw new IllegalStateException("select/distinct columns " + columns
                        + " cannot all be resolved even after isolation");
            }
        }
        return base.withProjections(ps);
    }

    /** Single-column relation removeDuplicates (rule owned by
     * ValueCollectionOps.relationDistinct) lowers as TypedDistinct. */
    private SqlSelect distinctOf(TypedNativeCall nc) {
        return distinct(Objects.requireNonNull(
                ValueCollectionOps.relationDistinct(nc)));
    }

    private SqlSelect distinct(TypedDistinct d) {
        // TDS union (the Typer's distinct-over-concatenation desugar):
        // the engine form — subselect(A union B) named by the unionAlias
        // frame, plain outer re-projection of the output columns (SQL
        // UNION dedups; never a distinct wrapper)
        // whole-row iff columns cover the SOURCE schema (output row is narrowed)
        boolean wholeRow = d.columns().isEmpty()
                || d.columns().equals((Type.requireRelationSchema(d.source().info().type()))
                        .columns().stream().map(Type.Column::name).toList());
        if (wholeRow && d.source() instanceof TypedConcatenate tc) {
            SqlUnion u = union(tc);
            SqlUnion dedup = new SqlUnion(u.branches(), false, u.outputs());
            SqlSource.Subselect sub = new SqlSource.Subselect(
                    dedup, nextAlias(), "unionAlias");
            List<SqlSelect.Projection> projs = new ArrayList<>();
            for (Type.Column c : (Type.requireRelationSchema(d.info().type()))
                    .columns()) {
                projs.add(new SqlSelect.Projection(
                        SqlExpr.Column.of(sub.alias(), u.outputs(),
                                c.name()), c.name(),
                        Fold.named(u.outputs(), c.name())));
            }
            return new SqlSelect(projs, false, sub, null, List.of(), null,
                    null, List.of(), null, null, List.of());
        }
        SqlSelect src = relation(d.source());
        if (!d.columns().isEmpty()) {
            return distinctNarrowTo(src, d.columns(), d.info());
        }
        return (Fold.distinctFolds(src) ? src : isolate(src)).withDistinct();
    }

    /**
     * rename lowers to a FULL explicit projection from the (always-known)
     * output schema &mdash; flat and self-describing; no EXCLUDE gymnastics.
     * Column ORDER is the checker's {@code T-Z+V} (real pure: renamed
     * columns move to the END) &mdash; iterating {@code r.info()} keeps the
     * SQL aligned with the typed schema the executor reads by index.
     */
    private SqlSelect rename(TypedRename r) {
        SqlSelect src = relation(r.source());
        // the engine FRAMES a rename of a PROJECTED source (persontable_0
        // reads the original TDS aliases); an already-framed source
        // absorbs chained renames; a BARE SCAN renames in place (the
        // relation-channel flat pin — nothing projected to frame)
        SqlSelect base = Fold.projectionFolds(src)
                && (src.from() instanceof SqlSource.Subselect
                        || src.projections().isEmpty())
                ? src : isolate(src);
        Type.RelationType outSchema = Type.requireRelationSchema(r.info().type());
        // Each output column reverse-maps to the source column it renames.
        Function<String, String> sourceOf = out -> {
            for (TypedRename.ColRename cr : r.renames()) {
                if (cr.to().equals(out)) {
                    return cr.from();
                }
            }
            return out;
        };
        // Pre-pass: if ANY source column would not resolve to a plain column
        // reference in the folded select, isolate ONCE, then project.
        for (Type.Column c : outSchema.columns()) {
            if (Fold.resolveInto(base, sourceOf.apply(c.name())) == null) {
                base = isolate(base);
                break;
            }
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (Type.Column c : outSchema.columns()) {
            String from = sourceOf.apply(c.name());
            SqlExpr e = Fold.resolveInto(base, from);
            if (e == null) {
                throw new IllegalStateException("rename source column '" + from
                        + "' cannot be resolved after isolation");
            }
            ps.add(new SqlSelect.Projection(e, c.name().equals(
                    e instanceof SqlExpr.Column col ? col.name() : null)
                            ? null : c.name(), Fold.slot(c, sqlTypeOf(c.type()))));
        }
        return base.withProjections(ps);
    }

    /** TDS literal → VALUES; empty → one all-NULL row gated by WHERE 1=0 (schema, zero rows). */
    private SqlSelect tdsLiteral(TypedTds tds) {
        Type.RelationType schema = Type.requireRelationSchema(tds.info().type());
        List<String> names = schema.columns().stream().map(Type.Column::name).toList();
        // The _tds alias names the literal's VALUES source (engine parity;
        // the shared counter keeps multiple literals distinct).
        String alias = "_tds" + tdsCounter++;
        if (tds.rows().isEmpty()) {
            List<SqlExpr> nulls = names.stream().map(n -> (SqlExpr) new SqlExpr.NullLit()).toList();
            SqlSource.Values v = new SqlSource.Values(List.of(nulls), names, alias, outputsOf(tds.info()));
            return SqlSelect.starOf(v).withWhere(SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlExpr.IntLit(1), new SqlExpr.IntLit(0)));
        }
        List<List<SqlExpr>> rows = new ArrayList<>(tds.rows().size());
        for (List<String> row : tds.rows()) {
            List<SqlExpr> cells = new ArrayList<>(row.size());
            for (int i = 0; i < row.size(); i++) {
                cells.add(Scalars.tdsCell(row.get(i), schema.columns().get(i).type()));
            }
            rows.add(cells);
        }
        return SqlSelect.starOf(new SqlSource.Values(rows, names, alias, outputsOf(tds.info())));
    }

    // ==================================================================
    // Joins — a structural SOURCE (JoinTree); sides bind per lambda param
    // ==================================================================

    /**
     * USER-facing {@code navigate(~alias: <relation>, {s,t|cond})} — the
     * clean-sheet dynamic navigation over relations: a PREFIXED LEFT join
     * (alias_COL columns), riding the exact TypedJoin machinery; struct
     * reads ({@code $r.alias.COL}) flatten in the scalar path. Class-extent
     * navigates are STORE material and never reach the lowerer.
     */
    private SqlSelect navigate(TypedNavigate nav) {
        Type.RelationType targetRel = Type.relationSchema(nav.target().info().type());
        if (nav.form() != TypedNavigate.Form.PRE_MAP
                || targetRel == null
                || nav.alias().isEmpty()) {
            throw new IllegalStateException("store-only navigate (class-extent"
                    + " target) reached the lowerer — resolver bug");
        }
        String alias = nav.alias().get();
        var srcRow = Type.requireRelationSchema(nav.source().info().type());
        Type.RelationType targetRow = targetRel;
        List<Type.Column> flat = new ArrayList<>(srcRow.columns());
        for (Type.Column c : targetRow.columns()) {
            flat.add(new Type.Column(navFlatColumn(alias, c.name()), c.type(),
                    Multiplicity.Bounded.ZERO_ONE));
        }
        var flatInfo = new ExprType(
                Type.relation(new Type.RelationType(flat)),
                nav.info().multiplicity());
        var leftKind = new TypedEnumValue(
                "meta::pure::functions::relation::JoinKind", "LEFT", nav.info());
        return join(new TypedJoin(nav.source(),
                nav.target(), leftKind, nav.predicate(),
                Optional.of(navSlotPrefix(alias)), null, flatInfo,
                false /* nav-slot synth */));
    }

    /** THE navigate flat-column convention ({@code slot_COL}): mint and
     * read-side reconstruction share this one owner (audit 15 — they were
     * spelled independently, the same drift class JoinIdentity retired). */
    private static String navSlotPrefix(String slot) {
        return slot + "_";
    }

    private static String navFlatColumn(String slot, String col) {
        return navSlotPrefix(slot) + col;
    }

    private SqlSelect join(TypedJoin j) {
        SqlSelect leftSel = relation(j.left());
        // A RENAME-ONLY select (star + plain column renames, no clauses —
        // what a PREFIXED join produces) can HOST further joins flat: its
        // join tree is the left side and its renames carry into the chain's
        // projections; refs to renamed columns in the ON condition
        // substitute to their underlying columns (the resolver's prefix
        // chains stay one flat SELECT — the real engine's shape).
        List<SqlSelect.Projection> leftCarry = null;
        SqlSource left;
        if (j.prefix().isPresent() && SqlProbes.isRenameOnlySelect(leftSel)) {
            // Hosting is only sound when the new join is PREFIXED — the
            // prefixed joined() branch re-emits the carry; the unprefixed
            // branch is SELECT * and would DROP the renames/narrowing
            // (audit blocker: rename->join lost its rename silently).
            leftCarry = leftSel.projections();
            left = leftSel.from();
            if (System.getenv("LEGEND_LITE_CARRY_TRACE") != null) {
                System.err.println("[carry] prefix=" + j.prefix().get()
                        + " carry=" + leftCarry.stream()
                                .map(SqlSelect.Projection::alias).toList());
            }
        } else {
            // a UNION-FRAMED operand keeps its frame identity through
            // join isolation (engine: joins against a union wrap it as
            // another unionAlias frame)
            left = unionFramed(leftSel)
                    ? new SqlSource.Subselect(leftSel, nextAlias(),
                            "unionAlias")
                    : asLeftJoinSide(leftSel);
        }
        SqlSelect rightSel = relation(j.right());
        SqlSource right = asRightSide(rightSel,
                unionFramed(rightSel) ? "unionAlias" : j.frameName());
        // a USER join lambda's ON keeps pure's null-safe equality (testJoinOnNullKey);
        // a RESOLVER-SYNTHESIZED join is the mapping's definition — plain '=' (slotDemandJoins)
        SqlExpr on;
        if (j.userCondition()) {
            on = sideCondition(j.condition(), left, right, leftCarry);
        } else {
            boolean prevVerbatim = verbatimEquality;
            verbatimEquality = true;
            try {
                on = sideCondition(j.condition(), left, right, leftCarry);
            } finally {
                verbatimEquality = prevVerbatim;
            }
        }
        SqlSource.Join.Kind kind = switch (j.kind().value()) {
            case "INNER" -> SqlSource.Join.Kind.INNER;
            case "LEFT" -> SqlSource.Join.Kind.LEFT;
            case "RIGHT" -> SqlSource.Join.Kind.RIGHT;
            case "FULL" -> SqlSource.Join.Kind.FULL;
            default -> throw new IllegalStateException("unknown join kind " + j.kind().value());
        };
        SqlSelect out = joined(new SqlSource.Join(left, right, kind, on),
                j.prefix(), j.right(), j.info(), leftCarry);
        // a join CONTAINING a union frame isolates as one more union
        // frame: select * from (lhs join rhs on ...) as "unionalias_N"
        // — downstream ops read the wrapper's outputs (engine model)
        if (unionSide(left) || unionSide(right)) {
            // §E3 M-N2: the wrapper INHERITS the joined frame's outputs
            // (pad-weakened) — starOf reads them straight off the
            // Subselect; re-asserting outputsOf(info) here would
            // re-echo the kind-blind contract (the SqlUnion ctor lesson)
            return SqlSelect.starOf(new SqlSource.Subselect(out, nextAlias(),
                    "unionAlias"));
        }
        return out;
    }

    private static boolean unionFramed(SqlSelect s) {
        return s.from() instanceof SqlSource.Subselect sub
                && "unionAlias".equals(sub.frameName());
    }

    private static boolean unionSide(SqlSource s) {
        return s instanceof SqlSource.Subselect sub
                && "unionAlias".equals(sub.frameName());
    }

    /** asOfJoin: DuckDB ASOF LEFT JOIN; ON = optional keys AND the match inequality. */
    private SqlSelect asOfJoin(TypedAsOfJoin aj) {
        SqlSource left = asLeftJoinSide(relation(aj.left()));
        SqlSource right = asRightSide(relation(aj.right()));
        SqlExpr on = sideCondition(aj.match(), left, right);
        if (aj.condition().isPresent()) {
            on = SqlExpr.Call.of(SqlFn.AND, sideCondition(aj.condition().get(), left, right), on);
        }
        Set<String> leftNames = new HashSet<>();
        Type.requireRelationSchema(aj.left().info().type()).columns().forEach(c -> leftNames.add(c.name()));
        return joined(new SqlSource.Join(left, right, SqlSource.Join.Kind.ASOF_LEFT, on),
                aj.prefix(), aj.right(), aj.info(), null, leftNames::contains);
    }

    /**
     * The joined select: bare star when column names are disjoint (Phase G
     * guarantees), or left star + explicitly re-aliased right columns when a
     * prefix renames EVERY right column.
     */
    private SqlSelect joined(SqlSource.Join source, Optional<String> prefix,
                             TypedSpec rightNode, ExprType info) {
        return joined(source, prefix, rightNode, info, null, name -> true);
    }

    private SqlSelect joined(SqlSource.Join source, Optional<String> prefix,
                             TypedSpec rightNode, ExprType info,
                             @com.legend.Nullable List<SqlSelect.Projection> leftCarry) {
        return joined(source, prefix, rightNode, info, leftCarry, name -> true);
    }

    private SqlSelect joined(SqlSource.Join source, Optional<String> prefix,
                             TypedSpec rightNode, ExprType info,
                             @com.legend.Nullable List<SqlSelect.Projection> leftCarry,
                             Predicate<String> renameWhen) {
        SqlSelect out = SqlSelect.starOf(source);
        if (prefix.isEmpty()) {
            // star frame: outputs are the SOURCE's own (Join.outputs()
            // carries the pad truth) — starOf already read them
            return out;
        }
        // Outputs-from-projections: the star carries the left side's
        // whole list; explicit columns attach their contract slot —
        // outputs right at birth (stampJoinOrigins is deleted).
        List<OutputCol> contract = outputsOf(info);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (leftCarry != null) {
            ps.addAll(leftCarry);   // the hosted chain's star + prior renames
        } else if (source.left() instanceof SqlSource.Join leftTree) {
            // A bare join tree has no single alias, and Star(null) would
            // expand the WHOLE FROM — leaking the new right side's
            // unprefixed columns (audit blocker). Enumerate the left
            // tree's columns explicitly (names are disjoint by the
            // Phase-G join invariant).
            for (OutputCol c : leftTree.outputs()) {
                ps.add(new SqlSelect.Projection(
                        Objects.requireNonNull(
                                Fold.sourceColumn(leftTree, c.name()),
                                c.name()), null, Fold.named(contract, c.name())));
            }
        } else {
            ps.add(new SqlSelect.Projection(
                    new SqlExpr.Star(source.left().alias()), null, null));
        }
        for (Type.Column c : Type.requireRelationSchema(rightNode.info().type()).columns()) {
            SqlExpr.Column rc = SqlExpr.Column.of(source.right().alias(),
                    source.right().outputs(), c.name());
            boolean renamed = renameWhen.test(c.name());
            String outName = renamed ? prefix.get() + c.name() : c.name();
            ps.add(new SqlSelect.Projection(
                    source.kind().padsRight() ? rc.asNullable() : rc,
                    renamed ? outName : null, Fold.named(contract, outName)));
        }
        return out.withProjections(ps);
    }

    /** A join side must be FROM-addressable: bare scans join directly,
     * claused selects wrap; a bare JOIN-select stays a bare tree ONLY on
     * the LEFT (SQL joins are left-associative). */
    /**
     * A join's LEFT side: a bare select unwraps to its source — including a
     * bare join TREE (SQL joins are left-associative, so chains stay flat).
     */
    private SqlSource asLeftJoinSide(SqlSelect side) {
        return isBareSelect(side) ? side.from() : new SqlSource.Subselect(side, nextAlias(), null);
    }

    /**
     * A join's RIGHT side (also pivot's source): a bare select unwraps ONLY
     * to a non-join source — a join tree on the right would re-associate.
     */
    SqlSource asRightSide(SqlSelect side) {
        return asRightSide(side, null);
    }

    /** {@code frameName}: the derived table's model identity (a view-
     * backed join target) — rides the Subselect for dialect grouping. */
    private SqlSource asRightSide(SqlSelect side, @com.legend.Nullable String frameName) {
        return isBareSelect(side) && !(side.from() instanceof SqlSource.Join)
                ? side.from()
                : new SqlSource.Subselect(side, nextAlias(), frameName);
    }

    /** No clause set — the select adds nothing over its source. */
    private static boolean isBareSelect(SqlSelect side) {
        return side.projections().isEmpty() && !side.distinct()
                && side.where() == null && side.groupBy().isEmpty() && side.having() == null
                && side.qualify() == null && side.orderBy().isEmpty()
                && side.limit() == null && side.offset() == null;
    }

    /**
     * The two-parameter condition: each lambda variable binds to its side.
     * A flat-chained left side is a join TREE — its refs resolve by walking
     * side schemas ({@link Fold#sourceColumn}), not by a single alias.
     */
    private SqlExpr sideCondition(TypedLambda lambda, SqlSource left, SqlSource right) {
        return sideCondition(lambda, left, right, null);
    }

    private SqlExpr sideCondition(TypedLambda lambda, SqlSource left, SqlSource right,
                                  @com.legend.Nullable List<SqlSelect.Projection> leftCarry) {
        String leftVar = lambda.parameters().get(0);
        return scalar(last(lambda), (var, prop) -> {
            boolean isLeft = leftVar.equals(var);
            if (prop == null) {
                // a WHOLE-VARIABLE read in a join condition has no column;
                // the pre-gate code NPE'd here — loud instead
                throw new IllegalStateException("join condition reads a whole"
                        + " variable '$" + var + "' — only column reads can correlate sides");
            }
            if (isLeft && leftCarry != null) {
                // A hosted chain's renamed column substitutes to its
                // underlying plain column (PF_OID -> t1.OID).
                for (SqlSelect.Projection pj : leftCarry) {
                    if (prop.equals(pj.outputName())
                            && pj.expr() instanceof SqlExpr.Column c) {
                        return c;
                    }
                }
            }
            SqlSource side = isLeft ? left : right;
            SqlExpr.Column c = side instanceof SqlSource.Join
                    ? Fold.sourceColumn(side, prop)
                    : SqlExpr.Column.of(side.alias(), side.outputs(), prop);
            if (c == null) {
                throw new IllegalStateException("join condition references unknown column '"
                        + prop + "' on its " + (isLeft ? "left" : "right")
                        + " side [side=" + Fold.describeSource(side) + "]");
            }
            return c;
        });
    }

    // ==================================================================
    // Window lowering — extend(over(...), ...) and whole-relation agg extend
    // ==================================================================

    /** extend(over(~p,[keys],frame), cols/aggs): window columns APPEND like extend. */
    private SqlSelect extendWindow(TypedExtendWindow w) {
        SqlSelect src = relation(w.source());
        SqlSelect base = Fold.windowFolds(src) ? src : isolate(src);
        SqlSelect out = foldOrIsolate(base, "extend window", b -> {
            Over over = lowerOver(b, w.window());
            List<OutputCol> contract = outputsOf(w.info());
            List<SqlSelect.Projection> ps = new ArrayList<>(starProjections(b));
            for (TypedFuncCol c : w.columns()) {
                SqlExpr e = windowScalar(last(c.fn()), b, over);
                ps.add(new SqlSelect.Projection(e, c.name(),
                        Fold.named(contract, c.name())));
            }
            for (TypedAggCol a : w.aggs()) {
                ps.add(new SqlSelect.Projection(
                        Windows.windowize(aggValue(b, a), over.partitionBy(), over.orderBy(),
                                over.frame()),
                        a.name(), Fold.named(contract, a.name())));
            }
            return b.withProjections(ps);
        });
        // a class-extent window closes its select (TypedExtendWindow
        // .extentBoundary): the query's filter lands in the OUTER WHERE
        return w.extentBoundary() ? isolate(out) : out;
    }

    /** extend(~total : x|$x.AGE : y|$y->sum()) — whole-relation window: SUM(x) OVER (). */
    private SqlSelect extendAgg(TypedExtendAgg ea) {
        SqlSelect src = relation(ea.source());
        SqlSelect base = Fold.windowFolds(src) ? src : isolate(src);
        SqlSelect out = foldOrIsolate(base, "extend aggregate", b -> {
            List<OutputCol> contract = outputsOf(ea.info());
            List<SqlSelect.Projection> ps = new ArrayList<>(starProjections(b));
            for (TypedAggCol a : ea.aggs()) {
                ps.add(new SqlSelect.Projection(
                        Windows.windowize(aggValue(b, a), List.of(), List.of(), null),
                        a.name(), Fold.named(contract, a.name())));
            }
            return b.withProjections(ps);
        });
        return ea.extentBoundary() ? isolate(out) : out;
    }

    private List<SqlSelect.Projection> starProjections(SqlSelect base) {
        if (!base.projections().isEmpty()) {
            return base.projections();
        }
        SqlExpr star = base.from() instanceof SqlSource.Join
                ? new SqlExpr.Star(null) : new SqlExpr.Star(base.from().alias());
        return List.of(new SqlSelect.Projection(star, null, null));
    }

    private record Over(List<SqlExpr> partitionBy, List<SqlSelect.SortKey> orderBy,
                        SqlExpr.WindowCall.@com.legend.Nullable Frame frame) {
    }

    /** Partition/order/frame of an over(...) — DESC→NULLS FIRST, ASC→NULLS LAST (master's pin). */
    private Over lowerOver(SqlSelect base, TypedOver over) {
        List<SqlExpr> parts = new ArrayList<>(over.partitions().size());
        for (String p : over.partitions()) {
            parts.add(resolveOrThrow(base, p));
        }
        List<SqlSelect.SortKey> keys = new ArrayList<>(over.sortKeys().size());
        for (TypedSort.TypedSortKey k : over.sortKeys()) {
            keys.add(new SqlSelect.SortKey(resolveOrThrow(base, k.column()), k.ascending(),
                    k.ascending() ? SqlSelect.SortKey.NullOrder.NULLS_LAST
                            : SqlSelect.SortKey.NullOrder.NULLS_FIRST, null));
        }
        return new Over(parts, keys, over.frame().map(Windows::sqlFrame).orElse(null));
    }

    /**
     * Whether a write destination reaches a PHYSICAL store table. A
     * TDS-accessor destination normalizes to the literal relation itself
     * (no table anywhere) — the write is vacuous and only the count is
     * observable.
     */
    private static boolean containsStoreTable(TypedSpec n) {
        if (n instanceof TypedTableReference
                || n instanceof TypedPackageableRef) {
            return true;
        }
        for (TypedSpec child : n.children()) {
            if (containsStoreTable(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A window column's body, classified AT LOWERING (the deliberate Phase-G
     * deferral): ranking natives take no column; value natives (lag/lead/...)
     * get their column from the WRAPPING property access
     * ({@code $p->lag($r).SALARY}); anything else lowers as an ordinary scalar
     * whose window-native subterms recurse through this method.
     */
    private SqlExpr windowScalar(TypedSpec body, SqlSelect base, Over over) {
        switch (body) {
            case TypedPropertyAccess p when p.source() instanceof TypedNativeCall call
                    && Windows.lookup(call.callee()) != null -> {
                Windows.WindowFn fn = Objects.requireNonNull(
                        Windows.lookup(call.callee()));
                List<SqlExpr> args = new ArrayList<>();
                // Resolve through the select — a folded project's column is
                // an ALIAS whose defining expression must substitute (a raw
                // FROM-qualified ref binds to nothing).
                args.add(resolveOrThrow(base, p.property()));
                trailingIntArgs(call, args);
                return new SqlExpr.WindowCall(new SqlAgg.ValueFn(fn.sqlName(), args),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            // reduce(rel, w, row, map, agg): the WINDOWED map+reduce — the
            // agg-col machinery arriving as a native call (real reduce.pure).
            // The synthetic TypedAggCol reuses aggValue's whole reducer
            // dispatch (rowMapper decomposition, composed aggs, casts), and
            // windowize stamps the shared window spec on every reducer.
            case TypedNativeCall call
                    when com.legend.builtin.NativeFn.LowererForm.of(call.callee().qualifiedName())
                            .orElse(null) == com.legend.builtin.NativeFn.LowererForm.REDUCE
                    && call.args().size() == 5
                    && call.args().get(3) instanceof TypedLambda mapFn
                    && call.args().get(4) instanceof TypedLambda aggFn -> {
                return Windows.windowize(aggValue(base, new TypedAggCol("_reduce", mapFn, aggFn, null, true)),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            // zScore(p,w,r,~col): COMPOSED window expression — real zScore.pure
            // is (col - average(...)) / max(stdDevPopulation(...), 1e-10).
            case TypedNativeCall call
                    when com.legend.builtin.NativeFn.LowererForm.of(call.callee().qualifiedName())
                            .orElse(null) == com.legend.builtin.NativeFn.LowererForm.Z_SCORE
                    && call.args().size() == 4
                    && call.args().get(3)
                            instanceof TypedColSpec zcs -> {
                SqlExpr col = resolveOrThrow(base, zcs.name());
                SqlExpr avg = new SqlExpr.WindowCall(
                        new SqlAgg.Reducer(SqlAgg.Fn.AVG, List.of(col), false, java.util.List.of()),
                        over.partitionBy(), over.orderBy(), over.frame());
                SqlExpr std = new SqlExpr.WindowCall(
                        new SqlAgg.Reducer(SqlAgg.Fn.STDDEV_POP, List.of(col), false, java.util.List.of()),
                        over.partitionBy(), over.orderBy(), over.frame());
                return SqlExpr.Call.of(SqlFn.DIVIDE,
                        SqlExpr.Call.of(SqlFn.MINUS, col, avg),
                        SqlExpr.Call.of(SqlFn.GREATEST, std,
                                new SqlExpr.DecimalLit(
                                        new java.math.BigDecimal("0.0000000001"))));
            }
            // Real pure's 4-arg colToAgg window aggregates: average(p,w,r,~col).
            case TypedNativeCall call when Windows.aggregate(call.callee()) != null -> {
                TypedSpec colArg = call.args().get(call.args().size() - 1);
                if (!(colArg instanceof TypedColSpec cs)) {
                    throw new IllegalStateException(
                            "window aggregate colToAgg must be a ~column colspec");
                }
                return new SqlExpr.WindowCall(
                        new SqlAgg.Reducer(Objects.requireNonNull(
                                Windows.aggregate(call.callee())),
                                // resolve through the select — a folded
                                // project's alias substitutes its expression
                                List.of(resolveOrThrow(base, cs.name())),
                                false, java.util.List.of()),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            case TypedNativeCall call when Windows.lookup(call.callee()) != null -> {
                Windows.WindowFn fn = Objects.requireNonNull(
                        Windows.lookup(call.callee()));
                if (fn.kind() != Windows.Kind.RANKING) {
                    throw new IllegalStateException("window value function '"
                            + call.callee().qualifiedName()
                            + "' needs a property access naming its column");
                }
                List<SqlExpr> args = new ArrayList<>();
                trailingIntArgs(call, args);
                return new SqlExpr.WindowCall(new SqlAgg.RankingFn(fn.sqlName(), args),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            case TypedNativeCall call -> {
                List<SqlExpr> args = call.args().stream()
                        .map(a -> windowScalar(a, base, over)).toList();
                return NullSemantics.verbatim(verbatimEquality || legacyNullUnsafeEquals, Scalars.lower(call, args, features));
            }
            // The n-ary arithmetic carrier ($r.AGE - $p->lag($r).AGE spells
            // minus([a, b]) — upstream's variadic natives, batch 5 leg 5):
            // its elements stay on the WINDOW channel; the arithmetic rule
            // folds the literal run to the operator chain.
            case TypedCollection c -> {
                return listLiteral(c, e -> windowScalar(e, base, over));
            }
            // Thunk lambdas (if branches) stay on the WINDOW channel — their
            // bodies may hold lag/lead property accesses that plain scalar
            // lowering cannot place.
            case TypedLambda l when l.parameters().isEmpty() -> {
                return new SqlExpr.Lambda(l.parameters(), windowScalar(last(l), base, over));
            }
            // Casts keep the channel too (window bodies end in ->cast(@Date)).
            case TypedCast c -> {
                return cast(c, windowScalar(c.source(), base, over));
            }
            // if(cond, |then, |else) in a window body: CASE WHEN whose arms
            // stay on the window channel (lead/lag accesses inside branches).
            case TypedIf i -> {
                return new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(
                                windowScalar(i.condition(), base, over),
                                windowScalar(PureSql.thunkBody(i.thenBranch()), base, over))),
                        i.elseBranch().map(e -> windowScalar(PureSql.thunkBody(e), base, over))
                                .orElse(new SqlExpr.NullLit()));
            }
            default -> {
                return scalar(body, (v, name) -> resolveOrThrow(base, name));
            }
        }
    }

    /**
     * Literal Integer args (ntile n, lag/lead offset) ride along; variable
     * refs are the window params; anything else is LOUD, never dropped.
     */
    private static void trailingIntArgs(TypedNativeCall call, List<SqlExpr> args) {
        for (TypedSpec a : call.args()) {
            if (a instanceof TypedCInteger c) {
                args.add(new SqlExpr.IntLit(c.value().longValue()));
            } else if (!(a instanceof TypedVariable)) {
                throw new IllegalStateException("window function argument of kind "
                        + a.getClass().getSimpleName() + " is not supported (literals only)");
            }
        }
    }

    // ==================================================================
    // Scalar lowering (lambda bodies)
    // ==================================================================

    /** {@code columns} resolves (lambda variable, property) to a SQL expression in scope. */
    /** A constructed field's value in its slot's carrier (MixedEncoding.slotCarrier). */
    private SqlExpr slotValue(TypedSpec value, Type.Column c, ColumnResolver columns) {
        SqlExpr lowered = scalar(value, columns);   // its own SQL type is the carrier truth (a map-binder cell IS its struct)
        SqlType lt = lowered.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : sqlTypeOf(value.info().type());
        SqlType valueType = isMany(value) && lt instanceof SqlType.Array la ? la.element() : lt;   // the carrier rule reads the ELEMENT
        return MixedEncoding.slotCarrier(lowered, isMany(value), valueType, sqlTypeOf(c.type()), () -> "_slot" + aliasCounter++);
    }

    SqlExpr scalar(TypedSpec spec, ColumnResolver columns) {
        SqlExpr r = scalarInner(spec, columns);
        // stamp-vs-shape INVARIANT (throws; LL_STAMP_COUNT=1 = census mode)
        StampCensus.check(spec, r);
        return r;
    }

    private SqlExpr scalarInner(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            case TypedUserCall g when RowGetters.isRowGetter(g) -> RowGetters.read(g, columns);
            // A literal BEYOND long (the parser kept it a BigInteger)
            // renders as a plain numeric literal — DuckDB reads HUGEINT.
            case TypedCInteger c -> c.value() instanceof java.math.BigInteger big
                    ? new SqlExpr.DecimalLit(new java.math.BigDecimal(big))
                    : new SqlExpr.IntLit(c.value().longValue());
            case TypedCString c -> new SqlExpr.StringLit(c.value());
            case TypedCBoolean c -> new SqlExpr.BoolLit(c.value());
            // B8: exact digits emit as DecimalLit under the Float label (see CFloat)
            case TypedCFloat c -> c.exact() != null
                    ? new SqlExpr.DecimalLit(c.exact())
                    : new SqlExpr.FloatLit(c.value());
            case TypedCDecimal c ->
                    new SqlExpr.DecimalLit(c.value());
            // Date literals: full dates/timestamps render typed; PARTIAL
            // dates (year / year-month) compare as STRINGS in SQL (master's
            // pinned semantics) — represented as string literals here.
            case TypedCDate d -> MatchFold.dateLit(d.value(), dbTimeZone);
            // %latest VALUE = the engine's STRING sentinel (VERDICT
            // burn §FINAL); the PREDICATE keeps TemporalFrame's arm.
            case com.legend.compiler.spec.typed.TypedCLatestDate ignored ->
                    new SqlExpr.Cast(new SqlExpr.StringLit(
                            "9999-12-31T00:00:00.0000"),
                            com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT);
            // The EMPTY collection [] (Nil[0]) in scalar position IS SQL
            // NULL — a [0] value has no cell representation other than null
            // (the mapping enum decode chain's tail: CASE ... ELSE NULL).
            case TypedCollection c when c.elements().isEmpty() -> new SqlExpr.NullLit();
            // A HETEROGENEOUS literal — element LUB Any ([1, 'a']) or a class
            // LUB with NO canonical layout (mixed instance kinds meeting at
            // an abstract ancestor): each element wraps as variant JSON, the
            // one SQL carrier that keeps per-element runtime kinds (a raw
            // mixed array cannot even type).
            case TypedCollection c when c.info().type() instanceof Type.ClassType ct
                    && !PlatformTypes.isVariant(ct)
                    && !PlatformTypes.isNil(ct)
                    && classLayout.apply(ct).isEmpty() -> {
                // THE HETERO-LITERAL CLAIM (F10 3b, M4 RE-LAND on the
                // typed IR — zero compensations): a VALUE-LANE Any-LUB
                // literal collection whose every element SPELLS rides
                // the LITERAL carrier — six kinds disjoint BY GRAMMAR,
                // so temporal/Decimal equality inside Any is byte-
                // decidable (json erased exactly those kinds: to_json
                // of a date and of its string print are the SAME bytes
                // — witness AnyLiteralByteDecidabilityTest). The
                // original parking blocker DISSOLVED with the carrier-
                // rule reversal: rows.values rides the grid again, so
                // grid-extraction asserts never meet this carrier (the
                // lane gates: value-lane only, never a rowCells body).
                // Enum/instance/computed elements decline per-element
                // and the collection keeps the variant lane; Number-LUB
                // mixes keep variant BY CONTRACT (arithmetic must type;
                // the next arm — the §1R census receipt).
                boolean cellSlots = c.rowCells();
                if (!cellSlots && CollectionLanes.valueLane(c)
                        && !c.elements().isEmpty()) {
                    java.util.List<SqlExpr> spelled =
                            new java.util.ArrayList<>(c.elements().size());
                    for (TypedSpec el : c.elements()) {
                        SqlExpr s = MixedEncoding.elementLiteral(
                                el, scalar(el, columns));
                        if (s == null) {
                            spelled = null;
                            break;
                        }
                        spelled.add(s);
                    }
                    if (spelled != null) {
                        yield ValueCollections.c1Singleton(c)
                                ? new SqlExpr.Cast(spelled.get(0),
                                        SqlType.Scalar.LITERAL)
                                : new SqlExpr.Cast(
                                        new SqlExpr.ArrayLit(spelled),
                                        new SqlType.Array(
                                                SqlType.Scalar.LITERAL));
                    }
                }
                // C1 collapse (ValueCollections.c1Singleton — witness
                // in::H2Test); the VARIANT carrier stays, the box goes.
                if (ValueCollections.c1Singleton(c)) {
                    var e0 = c.elements().get(0);
                    yield MixedEncoding.variantElement(e0,
                            scalar(e0, columns), cellSlots);
                }
                SqlExpr varr = new SqlExpr.ArrayLit(c.elements().stream()
                        .map(e -> MixedEncoding.variantElement(
                                e, scalar(e, columns), cellSlots))
                        .toList());
                // the plain arm's literal FLATTENING (a conditional-membership residual)
                yield !cellSlots && MixedEncoding.compacts(c) ? new SqlExpr.CompactList(varr) : varr;
            }
            // A NUMBER-LUB LITERAL mix ([25.0, 1]): a raw SQL array would
            // coerce every element to one numeric type (1 -> 1.0) — the
            // variant carrier keeps each element's own kind (pure
            // Number[*] semantics: the Integer stays an Integer).
            // LITERALS ONLY: computed elements keep the plain array so
            // downstream aggregates still type (testDivideFunctionPrecision)
            case TypedCollection c when c.info().type() == Type.Primitive.NUMBER
                    && c.elements().stream()
                            .map(e -> e.info().type()).distinct().count() > 1
                    && c.elements().stream().allMatch(e ->
                            e instanceof com.legend.compiler.spec.typed.TypedCInteger
                            || e instanceof com.legend.compiler.spec.typed.TypedCFloat
                            || e instanceof com.legend.compiler.spec.typed.TypedCDecimal) ->
                    new SqlExpr.ArrayLit(c.elements().stream()
                            .map(e -> (SqlExpr) SqlExpr.Call.of(
                                    SqlFn.TO_VARIANT, scalar(e, columns)))
                            .toList());
            case TypedCollection c -> {
                // HETEROGENEOUS Pair elements (Pair<String,String> with
                // Pair<String,Integer>: LUB Pair<String,Any>): every element
                // CASTS to the LUB's struct shape or the array cannot type
                if (c.info().type() instanceof Type.GenericType lubG
                        && PlatformTypes
                                .isPairCarrier(lubG)) {
                    boolean uniform = c.elements().stream()
                            .allMatch(e -> e.info().type().equals(c.info().type()));
                    if (!uniform) {
                        // rebuild each struct with per-field COERCION to the
                        // LUB: Any-typed slots take the variant carrier
                        // (to_json), never a text CAST
                        yield new SqlExpr.ArrayLit(c.elements().stream()
                                .map(e -> (SqlExpr) VariantShapes.pairToLub(scalar(e, columns),
                                        e.info().type(), lubG))
                                .toList());
                    }
                }
                // C1 collapse ([x] = x); consumers read stamps honestly.
                if (ValueCollections.c1Singleton(c)) {
                    yield scalar(c.elements().get(0), columns);
                }
                SqlExpr arr = listLiteral(c, e -> scalar(e, columns));
                // pure LITERAL FLATTENING at CONSTRUCTION (audit-of-R1:
                // consumer-site compaction was whack-a-mole — head/tail/
                // drop/take/makeString all read raw slots): an element
                // that CAN be empty ([0..1]/[0..*] stamped) contributes
                // nothing when empty, so the carrier compacts ONCE here
                // and every consumer sees the pure collection.
                // VALUE-LANE literals ONLY (corpus witness
                // testSelfJoinPropertyMapping): a ROW-cells list
                // ($r.values / hand-written property reads) keeps its
                // NULL cells — TDSNull is DATA on the grid convention.
                yield MixedEncoding.compacts(c) ? new SqlExpr.CompactList(arr) : arr;
            }
            // $r.alias.COL — a NAVIGATE slot's struct column flattens to
            // its prefixed physical column (alias_COL).
            case TypedPropertyAccess p when p.source() instanceof TypedPropertyAccess inner
                    && inner.source() instanceof TypedVariable v
                    && inner.info().type() instanceof Type.RelationType
                    -> {
                String flat = navFlatColumn(inner.property(), p.property());
                yield Objects.requireNonNull(columns.resolve(v.name(), flat),
                        () -> "unresolvable navigate column '" + flat + "'");
            }
            // A let-bound VALUE's field ($person.firstName after
            // |let person = ^Person(…)): extract from the lowered binding —
            // there is no row scope to resolve against.
            case TypedPropertyAccess p when p.source() instanceof TypedVariable v
                    && letBindings.containsKey(v.name()) -> {
                SqlExpr bound = Objects.requireNonNull(
                        letBindings.get(v.name()));
                // a field read through a plan parameter IS a dotted
                // placeholder — its KIND follows the FIELD type
                yield bound instanceof SqlExpr.PlanParam pp
                        ? new SqlExpr.PlanParam(pp.name() + "." + p.property(),
                                Fold.planKindOf(p.info().type()))
                        : new SqlExpr.StructGet(bound, p.property());
            }
            case TypedPropertyAccess p when p.source() instanceof TypedVariable v
                    -> Objects.requireNonNull(
                            columns.resolve(v.name(), p.property()),
                            () -> "unresolvable property '" + p.property()
                                    + "' on '$" + v.name() + "'");
            // Field access on a CLASS-typed VALUE (an instance literal, a
            // native call's struct result, a nested pair): the visible-literal
            // case inlines the field's own expression (no struct round-trip);
            // anything opaque extracts from the struct.
            // A property CHAIN over to(@Class) on a VARIANT (to(@Firm).boss.name):
            // every hop is a JSON field extraction; only the LEAF materializes
            // (real PCT testToClassAndAccessNestedProperty pins multi-hop).
            case TypedPropertyAccess p when VariantShapes.variantCastBase(p) != null -> {
                ArrayDeque<String> path = new ArrayDeque<>();
                TypedSpec cur = p;
                while (cur instanceof TypedPropertyAccess pa) {
                    path.addFirst(pa.property());
                    cur = pa.source();
                }
                var vc = (TypedCast) cur;
                SqlExpr e = scalar(vc.source(), columns);
                for (String seg : path) {
                    e = SqlExpr.Call.of(SqlFn.VARIANT_GET, e,
                            new SqlExpr.StringLit(seg));
                }
                Type leaf = p.info().type();
                if (leaf instanceof Type.ClassType lc
                        && !PlatformTypes.isVariant(lc)
                        && !PlatformTypes.isAny(lc)) {
                    // a class-typed LEAF is the unsupported-column case —
                    // real relation runtime's verbatim rejection
                    throw new ModelException(
                            LegendCompileException.Phase.LOWER,
                            "The type " + lc.fqn() + " is not supported yet!");
                }
                yield p.info().multiplicity().isMany()
                        ? new SqlExpr.Cast(e,
                                new SqlType.Array(PureSql.type(leaf)))
                        : new SqlExpr.Cast(e, PureSql.type(leaf));
            }
            case TypedPropertyAccess p when p.source()
                        instanceof TypedNewInstance ni -> {
                TypedSpec v = ni.properties().get(p.property());
                // The MODEL declares the field; an unset optional is NULL.
                yield v == null ? new SqlExpr.NullLit() : scalar(v, columns);
            }
            default -> scalarStructural(spec, columns);
        };
    }

    /** Scalar lowering, arm group (sequential order preserved:
     * guarded patterns depend on it) — the 523-line dispatch split
     * at arm boundaries; each group defaults to the next. */
    private SqlExpr scalarStructural(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            // Field access over a TO-MANY class value (filter(...).legalName)
            // MAPS the extraction; a to-one source extracts directly.
            // List.values over the bare-list carrier is the IDENTITY.
            case TypedPropertyAccess p when "values".equals(p.property())
                    && PlatformTypes
                            .isListCarrier(p.source().info().type()) ->
                    scalar(p.source(), columns);
            case TypedPropertyAccess p when classLayout.apply(p.source().info().type()).isPresent()
                    && isMany(p.source()) ->
                    manyPropertyMap(p, columns);
            case TypedPropertyAccess p when classLayout.apply(p.source().info().type()).isPresent()
                    -> {
                SqlExpr base = scalar(p.source(), columns);
                // a read THROUGH a plan parameter = the dotted placeholder
                String dotted = PlanParams.dottedPlanParam(base, p.property());
                yield dotted != null && !(p.info().type() instanceof Type.ClassType)
                        ? new SqlExpr.PlanParam(dotted, Fold.planKindOf(p.info().type()))
                        : new SqlExpr.StructGet(base, p.property());
            }
            // ^Class(prop=value, …) as a VALUE: a struct with the MODEL's
            // canonical layout (declared stored properties, declaration
            // order) — never the instance's own field set; an omitted
            // property is a NULL field.
            // ^$existing(prop=value, …): the copy is the source's canonical
            // struct with the overridden fields replaced — pure layout
            // rebuild, no new SQL shapes.
            case TypedCopyInstance cp -> {
                // the List CARRIER is a bare array, not its layout struct —
                // a values override replaces it wholesale; other platform
                // carriers reject loudly rather than emit the wrong shape
                if (PlatformTypes
                        .isListCarrier(cp.info().type())
                        || cp.classFqn().equals(
                                PlatformTypes.LIST)) {
                    TypedSpec ov = cp.overrides().get("values");
                    yield ov == null ? scalar(cp.source(), columns)
                            : PureSql.asList(scalar(ov, columns), isMany(ov));
                }
                if (PlatformTypes
                        .isMapCarrier(cp.info().type())) {
                    throw new NotImplementedException(
                            "^$var(…) copy of the Map carrier is not lowered");
                }
                var layout = classLayout.apply(cp.info().type()).orElseThrow(() ->
                        new IllegalStateException("^$var(…) copy of " + cp.classFqn()
                                + " has no canonical layout"));
                SqlExpr src = scalar(cp.source(), columns);
                yield ConstructionCanon.bind(instanceKeysOf, () -> aliasCounter++, cp.info().type(), cp.classFqn(), layout.stream().map(c -> {
                    // F13: a copy is a NEW instance — mint at the COPY site
                    var sf = MixedEncoding.syntheticField(c,
                            instanceIdOf == null ? null : instanceIdOf.apply(cp), cp.classFqn());
                    if (sf != null) {
                        return sf;
                    }
                    TypedSpec ov = cp.overrides().get(c.name());
                    SqlExpr v = ov != null ? slotValue(ov, c, columns)
                            : new SqlExpr.StructGet(src, c.name());
                    boolean manySlot = c.multiplicity() instanceof
                            Multiplicity.Bounded b && b.isMany();
                    if (ov != null && manySlot) {
                        v = PureSql.asList(v, isMany(ov));
                    }
                    // the DECLARED slot type rides the field (§4bZ-U
                    // leg 2 — same door as the ^new builder)
                    SqlType slot = sqlTypeOf(c.type());
                    return new SqlExpr.StructLit.Field(c.name(), v,
                            manySlot ? new SqlType.Array(slot) : slot);
                }).toList());
            }
            case TypedNewInstance n -> {
                // ^TDSNull() — the TDS null-cell VALUE (typed [1], never an
                // empty): its scalar REPRESENTATION is the SQL NULL literal,
                // exactly what the old sqlNull() funnel produced — every
                // comparison keeps its NullSemantics null-literal arm. The
                // VALUE-ness lives in the stamp: variant-lane collections
                // give a [1]-stamped element the json-null spelling so the
                // slot survives (grid convention — TDSNull is DATA).
                if (n.classFqn().equals(PlatformTypes.TDS_NULL_FQN)) {
                    yield new SqlExpr.NullLit();
                }
                // ^List(values=[...]): the List CARRIER is the bare SQL list
                // (the same carrier list() produces — one carrier per type).
                if (n.classFqn().equals(
                        PlatformTypes.LIST)) {
                    TypedSpec values = n.properties().get("values");
                    yield values == null
                            ? new SqlExpr.ArrayLit(List.of())
                            : PureSql.asList(scalar(values, columns), isMany(values));
                }
                // ^Pair(first=..., second=...): the Pair STRUCT carrier —
                // its layout IS first/second (the platform declaration)
                if (n.classFqn().equals(
                        PlatformTypes.PAIR)) {
                    yield new SqlExpr.StructLit(List.of(
                            new SqlExpr.StructLit.Field("first",
                                    scalar(Objects.requireNonNull(
                                            n.properties().get("first"),
                                            "Pair carries first"), columns)),
                            new SqlExpr.StructLit.Field("second",
                                    scalar(Objects.requireNonNull(
                                            n.properties().get("second"),
                                            "Pair carries second"), columns))));
                }
                // a property-less class's constructor: the synthetic fields alone
                var layout = classLayout.apply(n.info().type()).or(() -> n.properties().isEmpty()
                        ? Optional.of(com.legend.compiler.element.ClassLayouts.syntheticOnlyLayout(instanceIdOf != null))
                        : Optional.empty()).orElseThrow(() ->
                        new IllegalStateException("class value ^" + n.classFqn()
                                + "(…) has no canonical layout — the class declares no"
                                + " stored properties (or no model rides this lowering)"));
                yield ConstructionCanon.bind(instanceKeysOf, () -> aliasCounter++, n.info().type(), n.classFqn(), layout.stream().map(c -> {
                    // F13: one deterministic id per construction SITE (node)
                    var sf = MixedEncoding.syntheticField(c,
                            instanceIdOf == null ? null : instanceIdOf.apply(n), n.classFqn());
                    if (sf != null) {
                        return sf;
                    }
                    TypedSpec value = n.properties().get(c.name());
                    SqlExpr v = value == null ? new SqlExpr.NullLit() : slotValue(value, c, columns);
                    // A TO-MANY property is LIST-shaped in the canonical
                    // layout even when this instance supplies one value —
                    // every instance of a class shares ONE struct shape. The
                    // wrap decision uses the VALUE's typed multiplicity: an
                    // already-many expression ($p.nicknames) is a list even
                    // when it doesn't lower to a literal array (audit:
                    // structural-only check double-wrapped it).
                    boolean manySlot = c.multiplicity() instanceof
                            Multiplicity.Bounded b && b.isMany();
                    if (manySlot) {
                        v = PureSql.asList(v, value != null && isMany(value));
                    }
                    // the DECLARED slot type rides the field (§4bZ-U
                    // leg 2): an absent optional property's NULL still
                    // contributes its slot to the struct layout
                    SqlType slot = sqlTypeOf(c.type());
                    return new SqlExpr.StructLit.Field(c.name(), v,
                            manySlot ? new SqlType.Array(slot) : slot);
                }).toList());
            }
            // A bare variable: a query-level let binding substitutes; else a
            // lambda variable (a list element inside exists/forAll etc.).
            case TypedVariable v -> {
                SqlExpr bound = letBindings.get(v.name());
                yield bound != null ? bound
                        : Objects.requireNonNull(columns.resolve(v.name(), null),
                                () -> "unresolvable variable '$" + v.name() + "'");
            }
            // An inner lambda: ALL its parameters shadow; everything else
            // resolves outward through the enclosing resolver.
            case TypedLambda l -> new SqlExpr.Lambda(l.parameters(),
                    scalar(last(l), LambdaBinding.lambdaResolver(l.parameters(), columns)));
            // RELATION-level predicates — the true-SQL-EXISTS family
            // (collection natives over a Relation arg, correlated via the
            // enclosing scope stack): exists -> EXISTS(SELECT * WHERE p);
            // forAll -> NOT EXISTS(... WHERE NOT p) [vacuously true];
            // isEmpty/isNotEmpty -> [NOT] EXISTS; size -> COUNT(*).
            // A bare VARIABLE with a relation stamp is never a subquery:
            // a lambda binder holds a per-element CELL (stamp rides the
            // element) — it takes the scalar bridge.
            case TypedNativeCall n when RelationPredicates.applies(n) -> {
                var predicate = Objects.requireNonNull(RelationPredicates.of(n));
                enclosing.push(columns);
                try {
                    yield predicate.lower(this, n);
                } finally {
                    enclosing.pop();
                }
            }
            case TypedFold f -> LambdaBinding.lowerFold(this, f, columns);

            // map over a COLLECTION value -> listTransform (relation map is H).
            // pure map FLATTENS collection-valued mappers (audit 22a H3:
            // [1,2,3]->map(x|[$x,10*$x]) is the FLAT 6-element collection,
            // not 3 nested lists) — same TypedCollection-body policy as the
            // relation->map value-collection arm below.
            case com.legend.compiler.spec.typed.TypedSortBy sb
                    when !Type.relationValued(sb.source().info()) ->
                    ListEncodings.lowerSortBy(this, sb, columns);
            case TypedMap m
                    when !Type.relationValued(m.source().info()) ->
                    // the wire-shape policy AND the [0..0]-empty arm live
                    // with their owner (ListEncodings.lowerMap — the
                    // shape-limit seam split)
                    ListEncodings.lowerMap(this, m, columns);

            // Variant navigation: get(v, key) -> JSON access. The MAP
            // overload of the same bare name lowers through its own rule.
            case TypedNativeCall n when isFamily(n, "get")
                    && !PlatformTypes
                            .isMapCarrier(n.args().get(0).info().type()) ->
                    SqlExpr.Call.of(SqlFn.VARIANT_GET,
                            scalar(n.args().get(0), columns), scalar(n.args().get(1), columns));

            case TypedCast c -> cast(c, columns);

            case TypedSpec js when JsonEmission.owns(js) -> JsonEmission.lower(this, js, columns);
            // if(cond, {|then}, {|else}) — scalar position: CASE WHEN.
            // If-chains (the mapping enum decode emission) render as NESTED
            // CASE expressions in the otherwise slot — correct; single-CASE
            // flattening is a cosmetic peephole if ever demanded.
            case TypedIf i -> {
                TypedSpec thenB = PureSql.thunkBody(i.thenBranch());
                TypedSpec elseB = i.elseBranch()
                        .map(PureSql::thunkBody).orElse(null);
                // Any-LUB branch alignment (variant carrier) lives in
                // MixedEncoding.lubCase — the mixed-kind discipline.
                yield MixedEncoding.lubCase(i.info().type(), thenB, elseB,
                        scalar(i.condition(), columns),
                        scalar(thenB, columns),
                        elseB == null ? new SqlExpr.NullLit()
                                : scalar(elseB, columns));
            }

            // An enum VALUE in scalar position renders as its name string
            // (plangen :2591 parity; the mapping decode CASE compares against
            // these names, and result cells carry the name). Cross-type
            // equality (enum vs string / different enums) is guarded in the
            // equality arm below — silently-true 'NYC'=='NYC' across types
            // was an audit finding.
            case TypedEnumValue e -> new SqlExpr.StringLit(e.value());
            // an enum value's `.name` IS the value (the name string above)
            case TypedPropertyAccess pa when pa.property().equals("name")
                    && pa.source().info().type() instanceof Type.EnumType ->
                    scalarStructural(pa.source(), columns);

            // A bare TYPE REFERENCE in value position ([String, Integer]
            // vs columns.type asserts): type VALUES travel as canonical
            // simple names — the same wire convention as the type() fold
            // and the columnsMeta strings they compare against.
            case com.legend.compiler.spec.typed.TypedPackageableRef pr
                    when pr.info().type() instanceof
                            com.legend.compiler.element.type.Type.GenericType g
                    && g.rawFqn().equals(
                            com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS)
                    && g.arguments().size() == 1 ->
                    new SqlExpr.StringLit(MixedEncoding.simpleName(
                            g.arguments().get(0).typeName()));

            default -> scalarRelationalArms(spec, columns);
        };
    }

    /** Scalar lowering, arm group (sequential order preserved:
     * guarded patterns depend on it) — the 523-line dispatch split
     * at arm boundaries; each group defaults to the next. */
    private SqlExpr scalarRelationalArms(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            // Real pure equality is TYPE-aware: an enum equals nothing of
            // a different enum or non-string kind — static FALSE (never
            // cross-enum name coincidence or a DB conversion error);
            // enum-vs-STRING keeps the corpus's name-comparison
            // convention. Class-instance eq is REFERENCE identity in real
            // pure — unrecoverable here (serialization erases identity;
            // PCT inlines captured instances by value): instances keep
            // struct comparison, identity tests ledgered.
            case TypedNativeCall n when (isFamily(n, "equal") || isFamily(n, "eq"))
                    && InstanceEquality.staticallyDisjoint(n.args()) -> new SqlExpr.BoolLit(false);

            // COLLECTION-VALUED relation nodes in scalar position (the
            // list encodings; relation-typed sources take relation()).
            // C1: a scalar-stamped source conforms by EMISSION (asList).
            case TypedFilter f when !Type.relationValued(f.source().info()) ->
                    SqlExpr.Call.of(SqlFn.LIST_FILTER,
                            PureSql.asList(scalar(f.source(), columns),
                                    isMany(f.source())),
                            scalar(f.predicate(), columns));
            // slice(start, stop) — ListEncodings.slice owns the bounds
            // clamps and real pure's inverted-bounds error
            // slice/drop/take consume the LIST carrier — a to-one-stamped
            // source ([7], the c1 collapse) BOXES by stamp (DEEP_AUDIT §3:
            // six collection ops were hard Binder errors on singletons)
            case TypedSlice s when !Type.relationValued(s.source().info()) ->
                    ListEncodings.slice(
                            PureSql.asList(scalar(s.source(), columns),
                                    !CollectionLanes.c1Literal(s.source())),
                            scalar(s.start(), columns),
                            scalar(s.stop(), columns));
            // drop(n): the suffix from n+1; negative n drops nothing (PCT).
            case TypedDrop d when !Type.relationValued(d.source().info()) -> {
                SqlExpr src = PureSql.asList(scalar(d.source(), columns),
                        !CollectionLanes.c1Literal(d.source()));
                yield SqlExpr.Call.of(SqlFn.LIST_SLICE, src,
                        ListEncodings.onePlus(ListEncodings.clamp0(scalar(d.count(), columns))),
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, src));
            }
            // take(n): the prefix; negative n takes nothing (PCT) — the clamp
            // matters because DuckDB reads a negative bound FROM THE END.
            case TypedLimit t when !Type.relationValued(t.source().info()) ->
                    SqlExpr.Call.of(SqlFn.LIST_SLICE,
                            PureSql.asList(scalar(t.source(), columns),
                                    !CollectionLanes.c1Literal(t.source())),
                            new SqlExpr.IntLit(1),
                            ListEncodings.clamp0(scalar(t.count(), columns)));
            // A let in EXPRESSION position (a callee shape the statement
            // folder didn't reach): bind and yield — the let IS its value.
            case TypedLet l -> {
                SqlExpr v = scalar(l.value(), columns);
                letBindings.put(l.name(), v);
                yield v;
            }
            default -> scalarValueTailArms(spec, columns);
        };
    }

    /** Scalar lowering, tail arm group (same chain — see
     * {@link #scalarRelationalArms}; sequential order preserved). */
    private SqlExpr scalarValueTailArms(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            // makeString/joinStrings over the $r.values TDSRow-cells
            // synthesis (construction-declared — tc.rowCells()): stringify each
            // cell (TDSNull print convention, audit 9); hand-written
            // cell lists never match the roster test.
            case TypedNativeCall n
                    when (isFamily(n, "makeString") || isFamily(n, "joinStrings"))
                    && !n.args().isEmpty()
                    && n.args().get(0)
                            instanceof TypedCollection tc
                    && tc.rowCells() -> {
                // Statically-enumerated cells: a static CONCAT interleave
                // (ValueCollections.rowCellsJoin) — no list machinery
                // (burn-to-zero: the old ArrayLit-and-delegate form had
                // TWO owners for one encoding).
                List<SqlExpr> cells = new ArrayList<>(tc.elements().size());
                for (TypedSpec e : tc.elements()) {
                    cells.add(SqlExpr.Call.of(SqlFn.COALESCE,
                            Fold.cellText(e.info().type(), scalar(e, columns)),
                            new SqlExpr.StringLit(com.legend.compiler.element.type
                                        .PlatformTypes.TDS_NULL_CELL)));
                }
                List<SqlExpr> rest = new ArrayList<>();
                for (int i = 1; i < n.args().size(); i++) {
                    rest.add(scalar(n.args().get(i), columns));
                }
                yield ValueCollections.rowCellsJoin(cells, rest);
            }
            // statically-decided instanceOf folds (Scalars owns the rule)
            case TypedNativeCall n
                    when isFamily(n, "instanceOf") && n.args().size() == 2 ->
                    Scalars.instanceOfFold(n);
            // removeDuplicates/sort over a single-column RELATION read
            // rewrite to RELATION space (ValueCollectionOps — list-space
            // rules would re-embed the list subquery in a SQL lambda)
            case TypedNativeCall n
                    when ValueCollectionOps.relationSpaceRewrite(n) != null ->
                    scalar(Objects.requireNonNull(
                            ValueCollectionOps.relationSpaceRewrite(n)), columns);
            // F4.2 (RENDER): toCSV is a plan PROJECTION the DB executes
            case TypedNativeCall tc when PlatformTypes.TO_CSV
                    .equals(tc.callee().qualifiedName()) ->
                Render.lowerToCsv(tc, this::relation, nextAlias(), deferredTds);
            // the TDS class's csv property (tds.pure:19 `csv: String[1]`)
            // over a relation: the csv TEXT the engine prints for a TDS —
            // header names joined ', ', ','-joined cells with TDSNull, no
            // trailing newline (enumeration golden testEnumInRelation)
            case TypedPropertyAccess csvRead
                    when csvRead.property().equals(PlatformTypes.TDS_CSV_PROPERTY)
                    && Type.relationSchema(csvRead.source().info().type()) != null ->
                Render.lowerTdsCsvProperty(csvRead, this::relation, nextAlias());
            // F4.2c (RENDER): relation toString — the '#TDS' text form
            case TypedNativeCall tc when
                    "meta::pure::functions::relation::toString"
                            .equals(tc.callee().qualifiedName()) ->
                Render.lowerToString(tc, this::relation, nextAlias(),
                        deferredTds);
            // F13c — eq/equal over INSTANCE operands (identity lane):
            // ONE owner of the equality relation — the same canon the
            // verdict layer byte-judges with. eq = identity (__id
            // compare); equal = key-tree canon (keyed) or identity
            // canon (keyless). An unclaimed shape falls to the generic
            // rule (the candidate check is type-only, side-effect-free).
            case TypedNativeCall n when instanceKeysOf != null
                    && InstanceEquality.claims(n) -> {
                SqlExpr ie = InstanceEquality.lower(n, instanceKeysOf,
                        this::sqlTypeOf, s -> scalar(s, columns),
                        () -> "_iq" + aliasCounter++);
                yield ie != null ? ie : NullSemantics.verbatim(verbatimEquality || legacyNullUnsafeEquals, Scalars.lower(n,
                        n.args().stream().map(a -> scalar(a, columns)).toList(), features));
            }
            // arg lowering rides the unary-lambda binding convention
            // (LambdaBinding — M4's replacement for the parked branch's
            // LambdaWire ThreadLocal): a unary lambda param carries the
            // preceding list's element wire, so dispatch inside bodies
            // sees the carrier at construction
            case TypedNativeCall n -> NullSemantics.verbatim(verbatimEquality || legacyNullUnsafeEquals, Scalars.lower(n,
                    LambdaBinding.lowerNativeArgs(n, columns, this::scalar), features));
            // write(rel, accessor) returns the COUNT of rows written (the
            // PCT contract) — Render.writeCount; a REAL store destination
            // stays loud until the insert path exists.
            case TypedWrite w -> {
                if (!(w.destination().isEmpty()
                        || !containsStoreTable(w.destination().get()))) {
                    throw new NotImplementedException(
                            "TypedWrite to a store destination is not yet implemented");
                }
                yield Render.writeCount(relation(w.source()), nextAlias());
            }
            // A CLASS REFERENCE in scalar position carries its SIMPLE name
            // (PCT: STR_Person->toString() == 'STR_Person').
            case TypedPackageableRef ref -> {
                String fqn = ref.fullPath();
                int idx = fqn.lastIndexOf("::");
                yield new SqlExpr.StringLit(idx < 0 ? fqn : fqn.substring(idx + 2));
            }
            // from() in scalar position: execution-context metadata only —
            // the value is its source's
            case TypedFrom fr2 ->
                    scalar(fr2.source(), columns);
            // relation->map(row|scalar) consumed as a VALUE COLLECTION
            // (makeString/joinStrings tails): aggregate the projected
            // column to a LIST value via a scalar subquery
            case TypedMap m2
                    when Type.relationValued(m2.source().info())
                    && m2.mapper() instanceof TypedLambda ml2
                    && !Type.isRelation(ml2.functionType().result().type()) -> {
                Multiplicity colMult2 = ml2.functionType().result().multiplicity();
                SqlSelect proj = relation(ValueCollections.valueColumnProject(
                        m2.source(), ml2, m2.info().type(), colMult2));
                // pure map FLATTENS collection-valued mappers ($r.values):
                // the list-of-cell-arrays flattens one level
                boolean collMapper = ValueCollections.isCollectionMapper(ml2);
                // C2-i (STAMP_DISCIPLINE_PROGRAM): a PROVABLY single-row
                // source (a LIMIT<=1 chain or Dual) with a scalar mapper
                // reads its cell as a PLAIN scalar subquery — the LIST
                // collect was the census's shape lie; zero rows stay SQL
                // NULL in both forms, and the scalar form carries the
                // DB-native single-row semantics.
                // ALSO requires the node's OWN stamp scalar: many-stamps
                // keep the LIST-box carrier (loose [*] over one value).
                if (!collMapper && Fold.provablySingleRow(proj)
                        && m2.info().multiplicity()
                                instanceof Multiplicity.Bounded mb2
                        && mb2.upper() != null && mb2.upper() <= 1) {
                    yield new SqlExpr.ScalarSubquery(proj);
                }
                // COLLECTION-MAPPER cells with scalar per-cell STAMPS lower
                // as true scalars (C1); the flatten contract needs list-of-
                // lists, so the collect re-boxes each cell BY STAMP.
                boolean scalarCells = collMapper
                        && ml2.body().get(ml2.body().size() - 1).info()
                                .multiplicity()
                                instanceof Multiplicity.Bounded cellB
                        && cellB.upper() != null && cellB.upper() <= 1;
                SqlExpr collected = ValueCollections.collectAsList(proj,
                        collMapper, scalarCells, nextAlias());
                // NULL-DROP (audit §5): LIST() keeps NULL cells, so an
                // optional-cell collect COMPACTS (order untouched).
                yield m2.info().multiplicity().isMany() && !collMapper
                        && Fold.optionalScalarCell(colMult2)
                        ? new SqlExpr.CompactList(collected)
                        : collected;
            }
            // A COLUMN READ over a relation chain in scalar position
            // ($tds.rows.id — the TDS getter desugar): narrow to the one
            // column and take the single-column relation route below.
            // relation-rooted OR a single ROW pick (at(0) over rows —
            // bare struct, to-one stamp): both narrow to the one column
            // and take the single-column relation route below.
            case TypedPropertyAccess pa
                    when Type.schemaView(pa.source().info().type())
                            instanceof Type.RelationType prt -> {
                Type.RelationType.Column c = Fold.scalarReadColumn(prt, pa.property());
                yield scalar(new TypedSelect(pa.source(),
                        List.of(pa.property()),
                        new ExprType(
                                Type.relation(new Type.RelationType(List.of(c))),
                                pa.info().multiplicity())), columns);
            }
            // A single-column RELATION consumed in SCALAR position. A
            // TO-ONE read is the correlated scalar subquery (value-position
            // filtered navigation): DuckDB raises on more than one row
            // (pure toOne semantics); an empty result is NULL ([0..1]). A
            // TO-MANY read is a VALUE COLLECTION (contains/in/makeString
            // consumers): aggregate the column to a LIST — the bare scalar
            // subquery would raise on the second row. The OUTER row
            // resolver rides the enclosing channel either way.
            // MULTI-column = ROW-MAJOR cell flatten (the whole-TDS assert
            // idiom); no toOne carve-out — a [1] stamp on a relation VALUE
            // is the value's mult, not the row count.
            // any relation-ish value in scalar position: a table, the
            // .rows collection, or ONE ROW (an at()-pick — its wire
            // form is the single-row relation flatten). Row lambda
            // VARIABLES never reach here (the column-resolver arms
            // above own them).
            case TypedSpec rel when Type.schemaView(rel.info().type())
                            instanceof Type.RelationType rt
                    && !rt.columns().isEmpty() -> {
                enclosing.push((v, name) -> {
                    SqlExpr r = columns.resolve(v, name);
                    if (r == null) {
                        throw new UnfoldableRef(
                                name == null ? "<whole variable>" : name);
                    }
                    return r;
                });
                try {
                    if (rt.columns().size() > 1) {
                        yield new SqlExpr.ScalarSubquery(
                                ValueCollections.rowMajorCellList(
                                        relation(rel), rt, nextAlias()));
                    }
                    // a RELATION-OP head (TDS distinct/restrict splice) is
                    // a VALUE COLLECTION whatever its stamp — [1] is the
                    // relation VALUE's mult, not the row count; the
                    // correlated-scalar route serves [0..1] nav encodings
                    // a [0..1]-STAMPED single-column DISTINCT is the
                    // graph-leaf scalar subquery (D6: the engine's
                    // dedup-then-hard-fail discipline replaced LIMIT 1;
                    // the backend's own >1-row error is the raise) —
                    // the TDS distinct/restrict SPLICE stays a value
                    // collection ([1]-stamped, the relation VALUE)
                    boolean zeroOneDistinct = rel instanceof TypedDistinct
                            && rel.info().multiplicity()
                                    instanceof com.legend.compiler.element
                                            .type.Multiplicity.Bounded zb
                            && zb.lower() == 0 && zb.upper() != null
                            && zb.upper() == 1;
                    boolean toMany = !zeroOneDistinct
                            && (rel instanceof TypedDistinct
                            || rel instanceof com.legend.compiler.spec.typed
                                    .TypedSort
                            || !(rel.info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded mb1
                            && mb1.isToOne()));
                    if (!toMany) {
                        yield new SqlExpr.ScalarSubquery(relation(rel));
                    }
                    SqlExpr listed = new SqlExpr.ScalarSubquery(
                            ValueCollections.columnList(relation(rel),
                                    rt.columns().get(0).name(), nextAlias()));
                    // NULL-DROP (audit §5): LIST() keeps NULL cells, so
                    // an optional-cell collect COMPACTS (order untouched).
                    yield Fold.optionalScalarCell(
                                    rt.columns().get(0).multiplicity())
                            ? new SqlExpr.CompactList(listed)
                            : listed;
                } finally {
                    enclosing.pop();
                }
            }
            // scalar-position graph value (H4 snapshot; SnapshotEnvelope)
            case TypedSerializeGraph g -> new SqlExpr.ScalarSubquery(
                    conformJsonEgress(SnapshotEnvelope.fold(
                            serializeGraph(g.asArrayWrapped(), false))));
            // static-dispatch match fold (MatchFold doc)
            case com.legend.compiler.spec.typed.TypedMatchRuntime mr ->
                    scalar(MatchFold.fold(mr), columns);
            // SANCTIONED frontier default — see relation() above.
            default -> throw new NotImplementedException("scalar lowering not yet implemented for "
                    + spec.getClass().getSimpleName());
        };
    }

    /** A resolver for positions where no row scope exists (literal evaluation). */
    ColumnResolver noScope() {
        return (var, name) -> {
            throw new IllegalStateException(
                    "an instance literal has no row scope for $" + var
                            + (name == null ? "" : "." + name));
        };
    }

    /**
     * pivot(~col, ~agg:...): DuckDB native PIVOT source. Single pivot column
     * (multi-column key synthesis is a later slice); aggregates via the same
     * reduce-overload dispatch as groupBy.
     */

    /**
     * flatten(~col): the column explodes via UNNEST in the select list —
     * schema-driven explicit projections (every other column plain, the
     * flattened one replaced). Downstream refs to the flattened column are
     * COMPUTED projections, so the fold policy isolates them naturally.
     * A Variant column casts to JSON[] first; a typed list unnests directly.
     */
    private SqlSelect flatten(TypedFlatten fl) {
        SqlSelect src = relation(fl.source());
        SqlSelect base = Fold.extendFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "flatten", b -> buildFlatten(b, fl));
    }

    private SqlSelect buildFlatten(SqlSelect base,
            TypedFlatten fl) {
        Type.RelationType schema = Type.requireRelationSchema(fl.source().info().type());
        List<OutputCol> contract = outputsOf(fl.info());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            SqlExpr col = resolveOrThrow(base, c.name());
            if (c.name().equals(fl.column())) {
                SqlExpr list = c.type() instanceof Type.ClassType
                        ? new SqlExpr.Call(SqlFn.VARIANT_ELEMENTS, List.of(col))
                        : col;
                ps.add(new SqlSelect.Projection(
                        new SqlExpr.Call(SqlFn.UNNEST, List.of(list)),
                        c.name(), Fold.named(contract, c.name())));
            } else {
                ps.add(new SqlSelect.Projection(col, null,
                        Fold.named(contract, c.name())));
            }
        }
        return base.withProjections(ps);
    }

    /**
     * to(@T) / toMany(@T) / cast(@T) in scalar position (all arrive as
     * {@code TypedCast}; multiplicity separates them):
     * <ul>
     *   <li>Variant source, scalar target: master's rule — a {@code ->} access
     *       becomes {@code ->>} (text extraction strips JSON quoting) before
     *       {@code CAST(... AS T)}.</li>
     *   <li>Variant source, many target ({@code toMany}): {@code CAST} to an
     *       array of the target; {@code @Variant} keeps JSON elements.</li>
     *   <li>Non-variant source: multiplicity/type erasure — identity.</li>
     * </ul>
     */
    private SqlExpr cast(TypedCast c,
                         ColumnResolver columns) {
        return cast(c, scalar(c.source(), columns));
    }

    /** The cast policy over an ALREADY-LOWERED source (CastPolicy owns every arm). */
    private SqlExpr cast(TypedCast c, SqlExpr value) {
        return CastPolicy.lower(c, value, isMany(c), engineText);
    }

    /** A Pure type with a direct scalar SQL carrier (primitives and sized decimals). */
    /**
     * A relation cast whose target columns ALL exist in the source projects
     * them (SQL-CAST where the type changed); any absent name means the
     * pivot idiom's dynamic columns — type-only pass-through.
     */
    private SqlSelect relationCast(TypedCast c,
                                   Type.RelationType srcRow, Type.RelationType tgtRow) {
        Map<String, Type.Column> src = new LinkedHashMap<>();
        for (Type.Column col : srcRow.columns()) {
            src.put(col.name(), col);
        }
        boolean allKnown = tgtRow.columns().stream()
                .allMatch(tc -> src.containsKey(tc.name()));
        SqlSelect base = relation(c.source());
        if (!allKnown) {
            // audit 23 C-b: the type-only pass-through is the PIVOT
            // dynamic-column idiom — a cast naming unknown columns over a
            // STATIC source is a typo and silently no-oping it mislabels
            // cells far from the cause
            if (srcRow.dynamicColumns() == null
                    || srcRow.dynamicColumns().isEmpty()) {
                throw new com.legend.error.NotImplementedException(
                        "cast(@Relation<...>) names column(s) absent from the"
                        + " STATIC source row " + tgtRow.columns().stream()
                                .map(Type.Column::name)
                                .filter(nm -> !src.containsKey(nm)).toList()
                        + " — only a dynamic-column (pivot) source admits"
                        + " unknown cast columns");
            }
            return base;   // dynamic (pivot) columns: re-type only
        }
        // Identity is POSITIONAL: a cast that merely REORDERS columns must
        // project (the executor matches result columns to the schema by
        // position — returning the source order would silently mislabel
        // cells; audit).
        boolean identity = tgtRow.columns().size() == srcRow.columns().size();
        for (int i = 0; identity && i < tgtRow.columns().size(); i++) {
            Type.Column tc = tgtRow.columns().get(i);
            Type.Column sc = srcRow.columns().get(i);
            identity = tc.name().equals(sc.name()) && tc.type().equals(sc.type());
        }
        if (identity) {
            return base;
        }
        SqlSelect first = Fold.projectionFolds(base) ? base : isolate(base);
        SqlSelect out = tryRelationCast(first, src, tgtRow, c);
        if (out == null) {
            out = tryRelationCast(isolate(first), src, tgtRow, c);
        }
        if (out == null) {
            throw new IllegalStateException("relation cast columns unresolvable"
                    + " even after isolation: " + tgtRow.typeName());
        }
        return out;
    }

    /** One pass; null when any target column would not fold against {@code base}. */
    private @com.legend.Nullable SqlSelect tryRelationCast(SqlSelect base, Map<String, Type.Column> src,
                                      Type.RelationType tgtRow,
                                      TypedCast c) {
        List<SqlSelect.Projection> ps = new ArrayList<>(tgtRow.columns().size());
        for (Type.Column tc : tgtRow.columns()) {
            switch (attempt(() -> resolveOrThrow(base, tc.name()))) {
                case Resolution.Resolved r -> {
                    Type.Column srcCol = src.get(tc.name());
                    if (srcCol == null) {
                        // gate-found NPE: a cast target column ABSENT from
                        // the source row silently NPE'd here — loud, naming
                        // the column (never a stack trace as the message)
                        throw new IllegalStateException("relation cast: target"
                                + " column '" + tc.name() + "' does not exist"
                                + " on the source row");
                    }
                    Type from = srcCol.type();
                    SqlExpr v = from.equals(tc.type())
                            || !CastPolicy.isSqlPrimitive(tc.type()) || !CastPolicy.isSqlPrimitive(from)
                            ? r.expr()
                            : new SqlExpr.Cast(r.expr(), PureSql.type(tc.type()));
                    ps.add(new SqlSelect.Projection(v, tc.name(),
                            Fold.slot(tc, sqlTypeOf(tc.type()))));
                }
                case Resolution.Unfoldable u -> {
                    return null;
                }
            }
        }
        return base.withProjections(ps);
    }

    /** Field access over a TO-MANY class value: MAP the extraction
     * (bound param — the attachment-site door), and FLATTEN when the
     * property itself is to-many — pure collections never nest
     * ([$p1,$p2].locations is the FLAT union); the FLATTEN's
     * NULL/empty-inner drop IS pure's empty-drop. The MODEL's declared
     * property multiplicity decides. */
    private SqlExpr manyPropertyMap(TypedPropertyAccess p,
            ColumnResolver columns) {
        String elem = "_pa" + aliasCounter++;
        SqlExpr paColl = scalar(p.source(), columns);
        SqlExpr mapped = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                paColl,
                SqlExpr.Lambda.bind(new SqlExpr.Lambda(List.of(elem),
                        new SqlExpr.StructGet(
                                SqlExpr.Column.derived(null, elem),
                                p.property())), paColl));
        boolean manyProp = classLayout
                .apply(p.source().info().type()).orElseThrow()
                .stream()
                .anyMatch(c -> c.name().equals(p.property())
                        && c.multiplicity().isMany());
        return manyProp
                ? SqlExpr.Call.of(SqlFn.LIST_FLATTEN, mapped)
                : mapped;
    }

    private static boolean isMany(TypedSpec spec) {
        return spec.info().multiplicity().requireBounded("lowering").isMany();
    }

    // fold lowering moved to LambdaBinding.lowerFold (the binding-door
    // owner) at the 3,500-line shape guard.

    // ==================================================================
    // Relation-level predicate family (EXISTS forms)
    // ==================================================================

    interface RelationPredicate {
        SqlExpr lower(Lowerer lowerer, TypedNativeCall call);
    }

    static boolean isFamily(TypedNativeCall n, String pureName) {
        // signatureKey membership — the LAST parser-node dispatch the re-audit
        // found dodging the parser-free wall (ArchUnit cannot see a dependency
        // reached through definition()'s return type + contains(Object)).
        return Pure.nativeNamed(pureName, n.callee().signatureKey());
    }

    /**
     * An EXISTS subquery projects the constant {@code 1} — its columns are
     * never read, and {@code SELECT 1} is the reference engines' lean shape
     * ({@code buildExistsPredicate}).
     */
    static SqlSelect select1(SqlSelect s) {
        return s.withProjections(List.of(new SqlSelect.Projection(
                new SqlExpr.IntLit(1), null, null)));
    }

    /** Lower {@code rel} and fold {@code pred} (negated for forAll) into its WHERE. */
    SqlSelect whereLambda(TypedSpec rel, TypedSpec predArg, boolean negate) {
        if (!(predArg instanceof TypedLambda lambda)) {
            throw new IllegalStateException("relation exists/forAll expects a predicate lambda");
        }
        SqlSelect sub = relation(rel);
        SqlExpr pred;
        if (tryPredicate(sub, lambda) instanceof Resolution.Resolved r) {
            pred = r.expr();
        } else {
            sub = isolate(sub);
            pred = predicateOrThrow(sub, lambda, "exists/forAll");
        }
        if (negate) {
            pred = SqlExpr.Call.of(SqlFn.NOT, pred);
        }
        // the exists PREDICATE is user-zone: it seeds the subselect's
        // conjuncts (engine buildExistsPredicate), correlation and
        // milestoning stamps order after it via the zone merge
        return sub.withWhere(mergeWhere(sub.where(), pred,
                com.legend.compiler.spec.typed.TypedFilter.Stamp.NONE));
    }

    // ==================================================================
    // Plumbing
    // ==================================================================

    SqlSelect isolate(SqlSelect s) {
        return SqlSelect.starOf(new SqlSource.Subselect(s, nextAlias(), null));
    }

    /** A scalar of the predicate's OWN row (the resolver a relation predicate
     *  was entered with — RelationPredicates' value argument). */
    SqlExpr enclosingScalar(TypedSpec s) {
        return scalar(s, Objects.requireNonNull(enclosing.peek(), "no enclosing resolver"));
    }

    static TypedSpec last(TypedLambda lambda) {
        return lambda.body().get(lambda.body().size() - 1);
    }

    /** DERIVED-origin convenience — the three PHYSICAL doors pass
     * the origin explicitly (names owned by an external reality). */
    List<OutputCol> outputsOf(ExprType info) {
        return outputsOf(info, OutputCol.Origin.DERIVED);
    }

    List<OutputCol> outputsOf(ExprType info, OutputCol.Origin origin) {
        Type.RelationType rt = Type.schemaView(info.type());
        if (rt == null) {
            return List.of();
        }
        // THE Pure→SQL type boundary: plans carry SQL types only. A
        // ROW-STRUCT column (a user navigate's slot) is typed nesting over a
        // FLAT physical reality — its outputs are the prefixed columns the
        // join actually emitted (alias_COL, all nullable: LEFT semantics).
        List<OutputCol> out = new ArrayList<>(rt.columns().size());
        for (Type.Column c : rt.columns()) {
            // a slot column types the target's bare row-struct
            Type.RelationType sub =
                    c.type() instanceof Type.RelationType r0 ? r0 : null;
            if (sub != null) {
                for (Type.Column sc : sub.columns()) {
                    // join-prefixed slot columns are INVENTED spellings
                    // even over a physical scan
                    out.add(new OutputCol(c.name() + "_" + sc.name(),
                            PureSql.type(sc.type()), true, false,
                            OutputCol.Origin.DERIVED));
                }
                continue;
            }
            out.add(new OutputCol(c.name(), sqlTypeOf(c.type()),
                    PureSql.nullable(c.multiplicity()), false, origin));
        }
        return out;
    }
}
