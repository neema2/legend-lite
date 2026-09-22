package com.legend.resolver;

import com.legend.compiler.element.type.PlatformTypes;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * Store-anchor reachability &mdash; ALL the reaches in one place (remediation
 * T3.1: two same-named copies with silently different descent rules used to
 * live in StoreResolver and ClassSources).
 *
 * <ul>
 *   <li>{@link #anchored} &mdash; full descent, memoized per resolver pass:
 *       the resolveNode guard predicate. Memo keys are node IDENTITIES;
 *       rebuilt nodes get fresh identities, spliced-unchanged subtrees hit
 *       the memo, and the map is NEVER iterated &mdash; so identity keying is
 *       exact and order-insensitive. This was the O(guards&times;n&sup2;)
 *       full rescan under every guard.</li>
 *   <li>{@link #containsGetAll} &mdash; the same full descent, un-memoized,
 *       for static contexts (SubQueryLift).</li>
 *   <li>{@link #anchoredInFlow} &mdash; pipeline-FLOW reach: a navigate
 *       SLOT's target is getAll-shaped BY CONVENTION (the legacyNavigate
 *       emission), so only flow getAlls demand nested resolution
 *       (ClassSources).</li>
 * </ul>
 */
final class Anchors {

    /** D3 (StoreResolver.trackedElementClass): a reference to a
     * registry-tracked element is a store anchor — the metaclass extent
     * restricted to the element's key. */
    private final java.util.function.Predicate<
            com.legend.compiler.spec.typed.TypedPackageableRef> elementRef;

    /** A constructed metamodel instance the store carries as rows
     * (ConstructedInstances) anchors exactly like an element reference. */
    private final java.util.function.Predicate<
            com.legend.compiler.spec.typed.TypedNewInstance> constructedRow;

    /** A plan handle whose nodes the store carries as rows (PlanRows)
     * anchors like a constructed instance. */
    private final java.util.function.Predicate<TypedNativeCall> planHandle;

    Anchors(java.util.function.Predicate<
            com.legend.compiler.spec.typed.TypedPackageableRef> elementRef,
            java.util.function.Predicate<
                    com.legend.compiler.spec.typed.TypedNewInstance> constructedRow,
            java.util.function.Predicate<TypedNativeCall> planHandle) {
        this.constructedRow = constructedRow;
        this.elementRef = elementRef;
        this.planHandle = planHandle;
    }

    private final java.util.IdentityHashMap<TypedSpec, Boolean> memo =
            new java.util.IdentityHashMap<>();

    /** Unresolved store anchor beneath {@code n} (full descent, memoized). */
    boolean anchored(TypedSpec n) {
        Boolean hit = memo.get(n);
        if (hit != null) {
            return hit;
        }
        boolean v = false;
        if (n instanceof TypedGetAll
                // leg 3.4 step 2: a PLANNED frame referenced by name is a
                // resolved relation root — the chain it stands for was
                // anchored, so every reader of an anchored chain (the csv
                // render, the relation ops) reads the reference the same
                || n instanceof com.legend.compiler.spec.typed.TypedFrameRef) {
            v = true;
        } else {
            for (TypedSpec c : n.children()) {
                if (c instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr) {
                    // an element reference anchors ONLY as the SOURCE of a
                    // navigation (D3); as an argument (from(mapping, rt),
                    // execute(f, mapping, rt), tableReference(db, …)) it
                    // is a value
                    if (elementRef.test(pr) && navigatesSource(n, c)) {
                        v = true;
                        break;
                    }
                    continue;
                }
                if (c instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                        && constructedRow.test(ni) && navigatesSource(n, c)) {
                    v = true;
                    break;
                }
                // a store TABLE identity (db->schema('S')->table('T'), through
                // toOne peels) anchors ONLY when a PROPERTY is navigated from
                // it (.columns, .name); as a bare argument — loadCsvToDbTable,
                // replaceTables pairs — it is the VALUE the structural
                // native consumes (StoreElementIdentity)
                if (n instanceof TypedPropertyAccess pa && pa.source() == c
                        && com.legend.compiler.spec.typed.StoreElementIdentity
                                .isTableIdentity(peelToOne(c))) {
                    v = true;
                    break;
                }
                if (c instanceof TypedNativeCall pn && planHandle.test(pn)
                        && navigatesSource(n, c)) {
                    v = true;
                    break;
                }
                if (c instanceof com.legend.compiler.spec.typed.TypedLambda
                        && functionBodyRead(n)) {
                    v = true;
                    break;
                }
                if (anchored(c)) {
                    v = true;
                    break;
                }
            }
        }
        memo.put(n, v);
        return v;
    }

    private final java.util.IdentityHashMap<TypedSpec, Space> spaceMemo =
            new java.util.IdentityHashMap<>();

    /**
     * THE space classifier (one definition; memoized like {@link #anchored}).
     * Reads only STRONG inputs — the checked types in {@code info()} and the
     * node kinds — never re-deriving what the Typer decided.
     */
    Space spaceOf(TypedSpec n) {
        Space hit = spaceMemo.get(n);
        if (hit != null) {
            return hit;
        }
        Space v = objectSpine(n) ? Space.OBJECT
                : anchored(n) ? Space.ANCHORED
                : Space.INERT;
        spaceMemo.put(n, v);
        return v;
    }

    /** Whether {@code c} sits in {@code n}'s SOURCE position — the object-
     * spine shapes (the same node kinds {@link #objectSpine} walks). */
    private static boolean navigatesSource(TypedSpec n, TypedSpec c) {
        return switch (n) {
            case TypedPropertyAccess pa -> pa.source() == c;
            case TypedMap m -> m.source() == c;
            case TypedFilter f -> f.source() == c;
            case TypedFrom fr -> fr.source() == c;
            case TypedLimit l -> l.source() == c;
            case TypedDrop d -> d.source() == c;
            case TypedSlice sl -> sl.source() == c;
            case TypedSortBy sb -> sb.source() == c;
            case com.legend.compiler.spec.typed.TypedCast tc -> tc.source() == c;
            case TypedNativeCall nc -> !nc.args().isEmpty() && nc.args().get(0) == c
                    && (ClassSorts.isFirstLike(nc) || isStaticAt(nc)
                            || StoreResolver.isClassToOne(nc)
                            || Pipelines.isClassDistinct(nc)
                            || ClassSorts.classSortOf(nc) != null
                            || isDeactivate(nc)
                            || nc.callee().qualifiedName().equals(
                                    Substitution.ELEMENT_TO_PATH_FQN));
            default -> false;
        };
    }

    private static TypedSpec peelToOne(TypedSpec v) {
        TypedSpec cur = v;
        while (cur instanceof TypedNativeCall c && StoreResolver.isClassToOne(c)) {
            cur = c.args().get(0);
        }
        return cur;
    }

    /** {@code at(coll, k)} with a LITERAL index — class-space slice. */
    static boolean isStaticAt(TypedNativeCall c) {
        return c.args().size() == 2
                && "meta::pure::functions::collection::at".equals(c.callee().qualifiedName())
                && c.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCInteger;
    }

    /** {@code evaluateAndDeactivate(x)} — a tree-as-value native that is
     * the IDENTITY over metamodel rows (the rows already are the
     * deactivated tree); transparent on the object spine. */
    static boolean isDeactivate(TypedNativeCall nc) {
        return nc.args().size() == 1 && nc.callee().qualifiedName()
                .equals("meta::pure::functions::meta::evaluateAndDeactivate");
    }

    /** The object-space spine rules (formerly StoreResolver.isObjectSpace). */
    private boolean objectSpine(TypedSpec source) {
        return switch (source) {
            case TypedGetAll ignored -> true;
            // an element REFERENCE of a tracked metaclass IS its row (D3)
            case com.legend.compiler.spec.typed.TypedPackageableRef pr
                    when elementRef.test(pr) -> true;
            // a store TABLE named by its accessors (db->schema('S')->table('T'))
            // IS its row in the system store (ElementReferences.storeTableKey)
            case com.legend.compiler.spec.typed.TypedUserCall uc
                    when com.legend.compiler.spec.typed.StoreElementIdentity.isTableIdentity(uc) -> true;
            // a CONSTRUCTED metamodel instance the store carries as rows
            case com.legend.compiler.spec.typed.TypedNewInstance ni
                    when constructedRow.test(ni) -> true;
            // a PLAN HANDLE whose nodes the store carries as rows (PlanRows)
            case TypedNativeCall pn when planHandle.test(pn) -> true;
            // a FUNCTION VALUE's body read ($f.expressionSequence over a
            // lambda) — its statements are rows (FunctionBodyRows)
            case TypedPropertyAccess pa when functionBodyRead(pa) -> true;
            // a CLASS-typed property HOP over an object-space chain IS
            // object space (the auto-map flatten re-roots at its target);
            // class-typed = bare or PARAMETERIZED class (Type.classFqn —
            // the spec's Mapping.enumerationMappings is EnumerationMapping<Any>)
            case TypedPropertyAccess pa
                    when Type.classFqn(pa.info().type()) != null ->
                    spaceOf(pa.source()) == Space.OBJECT;
            // ->map with a CLASS-result mapper stays in object space
            case TypedMap m
                    when Type.classFqn(m.mapper().functionType().result()
                            .type()) != null ->
                    spaceOf(m.source()) == Space.OBJECT;
            case TypedFrom fr -> spaceOf(fr.source()) == Space.OBJECT;
            // ->cast(@Sub) in chain position re-types the chain (the
            // total-membership rule, StoreResolver.collectOpChain)
            case com.legend.compiler.spec.typed.TypedCast c
                    when Type.classFqn(c.target()) != null ->
                    spaceOf(c.source()) == Space.OBJECT;
            case TypedFilter f -> spaceOf(f.source()) == Space.OBJECT;
            case TypedLimit l -> spaceOf(l.source()) == Space.OBJECT;
            case TypedDrop d -> spaceOf(d.source()) == Space.OBJECT;
            case TypedSlice sl -> spaceOf(sl.source()) == Space.OBJECT;
            case TypedSortBy sb -> spaceOf(sb.source()) == Space.OBJECT;
            case TypedNativeCall c when ClassSorts.isFirstLike(c) ->
                    spaceOf(c.args().get(0)) == Space.OBJECT;
            case TypedNativeCall c when isStaticAt(c) ->
                    spaceOf(c.args().get(0)) == Space.OBJECT;
            case TypedNativeCall c when StoreResolver.isClassToOne(c) ->
                    spaceOf(c.args().get(0)) == Space.OBJECT;
            case TypedNativeCall c when Pipelines.isClassDistinct(c) ->
                    spaceOf(c.args().get(0)) == Space.OBJECT;
            case TypedNativeCall c when ClassSorts.classSortOf(c) != null ->
                    spaceOf(c.args().get(0)) == Space.OBJECT;
            case TypedNativeCall c when isDeactivate(c) ->
                    spaceOf(c.args().get(0)) == Space.OBJECT;
            default -> false;
        };
    }

    /** Full descent for static contexts. */
    static boolean containsGetAll(TypedSpec n) {
        if (n instanceof TypedGetAll) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsGetAll(c)) {
                return true;
            }
        }
        return false;
    }

    /** Pipeline-FLOW reach: skips a navigate's conventionally-getAll target. */
    static boolean anchoredInFlow(TypedSpec n) {
        if (n instanceof TypedGetAll) {
            return true;
        }
        if (n instanceof TypedNavigate nav) {
            return anchoredInFlow(nav.source());
        }
        for (TypedSpec c : n.children()) {
            if (anchoredInFlow(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@code <lambda>.expressionSequence} — the function-value body read
     * the store serves as rows (FunctionBodyRows). */
    static boolean functionBodyRead(TypedSpec n) {
        return n instanceof TypedPropertyAccess pa
                && pa.property().equals("expressionSequence")
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedLambda;
    }

    /** Diagnostics (env-gated callers): a compact typed-tree print —
     * callee simple names, property reads, binders — depth-limited. */
    static String compact(TypedSpec n, int depth) {
        if (depth == 0) {
            return "…";
        }
        String head = switch (n) {
            case TypedNativeCall nc -> nc.callee().qualifiedName()
                    .substring(nc.callee().qualifiedName().lastIndexOf(':') + 1);
            case com.legend.compiler.spec.typed.TypedUserCall uc -> "user:" + uc.callee()
                    .qualifiedName().substring(uc.callee().qualifiedName().lastIndexOf(':') + 1);
            case TypedPropertyAccess pa -> "." + pa.property();
            case com.legend.compiler.spec.typed.TypedVariable v -> "$" + v.name();
            case com.legend.compiler.spec.typed.TypedCast c -> "cast@" + c.target();
            case com.legend.compiler.spec.typed.TypedLambda l -> "lambda" + l.parameters();
            default -> n.getClass().getSimpleName();
        };
        StringBuilder sb = new StringBuilder(head);
        var kids = n.children();
        if (!kids.isEmpty()) {
            sb.append('(');
            for (int i = 0; i < kids.size(); i++) {
                sb.append(i > 0 ? ", " : "").append(compact(kids.get(i), depth - 1));
            }
            sb.append(')');
        }
        return sb.toString();
    }

    /** The relation a TDS-surface identity erases to: {@code <relation>.rows}
     * (a relation's rows ARE the relation) and {@code cast(@TabularDataSet)}
     * over a relation (CastChecker's rule the typer could not apply to an
     * envelope read, which becomes a relation only at the splice) — seen
     * through stacked casts. Null when {@code n} is neither. */
    /** The one-element picks a test spells over a one-value envelope read
     * ({@code ->at(0)}, {@code ->toOne()}, {@code ->first()}). */
    private static final java.util.Set<String> ONE_ELEMENT_PICKS = java.util.Set.of(
            "meta::pure::functions::collection::at",
            "meta::pure::functions::multiplicity::toOne",
            "meta::pure::functions::collection::first");

    static @com.legend.base.Nullable TypedSpec tdsErase(TypedSpec n) {
        TypedSpec src = n instanceof TypedPropertyAccess pa && pa.property().equals("rows")
                ? pa.source() : n instanceof com.legend.compiler.spec.typed.TypedCast ? n : null;
        if (src == null) {
            return null;
        }
        TypedSpec cur = src;
        boolean peeled = false;
        while (cur instanceof com.legend.compiler.spec.typed.TypedCast tc
                && com.legend.compiler.element.type.PlatformTypes.isTdsType(tc.target())) {
            cur = tc.source();
            peeled = true;
        }
        if (!(n instanceof TypedPropertyAccess) && !peeled) {
            return null;   // an ordinary cast is not this shape
        }
        // `$result.values->at(0)->cast(@TabularDataSet)`: a helper whose
        // query parameter is declared FunctionDefinition<Any> erases the
        // envelope to Result<Any>, and the test re-asserts the ONE TDS
        // value through at(0)/toOne()/first() + the cast (engine
        // testDataGeneration loadAndTestExecution). The element pick over
        // the one-value envelope read is the read itself; the cast is the
        // user's relation assertion, proven where the envelope splices.
        if (peeled && cur instanceof TypedNativeCall pick
                && pick.args().size() >= 1
                && ONE_ELEMENT_PICKS.contains(pick.callee().qualifiedName())
                && (pick.args().size() == 1
                        || pick.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCInteger ci
                                && ci.value().longValue() == 0)
                && pick.args().get(0) instanceof TypedPropertyAccess vals
                && vals.property().equals("values")
                && vals.source().info().type()
                        instanceof com.legend.compiler.element.type.Type.GenericType rg
                && com.legend.compiler.element.type.PlatformTypes.RESULT.equals(rg.rawFqn())) {
            return vals;
        }
        return com.legend.compiler.element.type.Type.isRelation(cur.info().type())
                || com.legend.compiler.element.type.PlatformTypes.isTdsType(cur.info().type())
                ? cur : null;
    }

    /** A cast to a TDS-shaped type over a TDS-shaped chain is a type-level
     * no-op ({@code $result.values->cast(@TDS<Any>)}): the chain beneath
     * (PlatformTypes.isTdsShaped owns the shape). */
    static TypedSpec peelTdsCasts(TypedSpec n) {
        return com.legend.compiler.spec.ResultEnvelopeSplice.peelTdsCasts(n);
    }

    /** The TDS class's csv property (PlatformTypes.TDS_CSV_PROPERTY,
     * tds.pure:19) over a TDS-shaped chain (an executed result cast to
     * {@code TDS<Any>}): the read over the chain beneath the type-level
     * casts — the chain resolves structurally, the read rides along as the
     * lowerer's CSV render (Render.lowerTdsCsvProperty, which dispatches on
     * the same property constant over a schema-typed source). Null = not
     * that shape. */
    static @com.legend.base.Nullable TypedPropertyAccess tdsCsvRead(TypedPropertyAccess pa,
            java.util.function.Predicate<TypedSpec> anchored) {
        if (!pa.property().equals(PlatformTypes.TDS_CSV_PROPERTY)
                || !PlatformTypes.isTdsShaped(pa.source().info().type())) {
            return null;
        }
        TypedSpec chain = peelTdsCasts(pa.source());
        return anchored.test(chain)
                ? new TypedPropertyAccess(chain, PlatformTypes.TDS_CSV_PROPERTY, pa.info())
                : null;
    }

    /** The LL_TMP_DEBUG node dump suffix of a resolver wall message. */
    static String debugSuffix(TypedSpec n) {
        return System.getenv("LL_TMP_DEBUG") != null
                ? " <<" + compact(n, 8) + ">>" : "";
    }
}
