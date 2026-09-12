// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
/**
 * SYNTHETIC HEAD identities — filtered navigations lift to
 * {@code head#fN} chains (predicate parked for the join target),
 * two-dates-per-head splits mint {@code head#dN} (a separate join
 * identity per distinct date-set), and {@link #realHead} keeps every
 * model lookup transparent. Append-only across nested resolutions —
 * names are counter-unique; the registry is the ONE owner of the
 * '#'-suffix convention — {@link JoinIdentity} is the value type, the
 * string form exists only because heads travel as property names.
 */
final class SyntheticHeads {

    /** Function catalog — the AND-merge of stacked filter predicates
     * needs the one 2-arg {@code boolean::and} overload. */
    private final com.legend.compiler.element.ModelContext ctx;

    SyntheticHeads(com.legend.compiler.element.ModelContext ctx) {
        this.ctx = java.util.Objects.requireNonNull(ctx, "ctx");
    }

    /**
     * A join identity parsed from a head name. IDENTIFIER property names
     * cannot contain {@code '#'}; QUOTED property names (M3
     * {@code propertyName: (identifier | STRING)}) can — minting over one
     * throws loudly (the constructor guard), and a malformed suffix is a
     * loud resolver bug. RESIDUAL (documented, corpus-free): a quoted
     * property spelled exactly like a minted name ({@code 'emp#f0'})
     * decodes as synthetic — full closure needs registry-membership
     * decode. ALL encode/decode knowledge of the {@code #fN}/{@code #dN}/
     * {@code #cN} convention lives in this record.
     */
    record JoinIdentity(String prop, Kind kind, int seq) {
        enum Kind { PLAIN, FILTERED, DATED, CONCAT, POSITIONAL, UNION }

        JoinIdentity {
            if (prop.indexOf('#') >= 0) {
                // QUOTED pure property names are arbitrary strings (M3:
                // propertyName: (identifier | STRING)) — a real property
                // containing '#' must never silently masquerade as one of
                // our synthetic identities, and composed synthetics
                // (prop#cN#dM) are a resolver bug either way. LOUD.
                throw new IllegalStateException(
                        "synthetic-head identity over a property containing"
                                + " '#' (quoted-name property or composed"
                                + " synthetic — resolver bug): " + prop);
            }
        }

        static JoinIdentity of(String head) {
            int i = head.indexOf('#');
            if (i < 0) {
                return new JoinIdentity(head, Kind.PLAIN, -1);
            }
            char k = head.charAt(i + 1);
            Kind kind = switch (k) {
                case 'f' -> Kind.FILTERED;
                case 'd' -> Kind.DATED;
                case 'c' -> Kind.CONCAT;
                case 'p' -> Kind.POSITIONAL;
                case 'u' -> Kind.UNION;
                default -> throw new IllegalStateException(
                        "malformed synthetic head (resolver bug): " + head);
            };
            int seq;
            try {
                seq = Integer.parseInt(head.substring(i + 2));
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                        "malformed synthetic head (resolver bug): " + head);
            }
            return new JoinIdentity(head.substring(0, i), kind, seq);
        }

        String encoded() {
            return switch (kind) {
                case PLAIN -> prop;
                case FILTERED -> prop + "#f" + seq;
                case DATED -> prop + "#d" + seq;
                case CONCAT -> prop + "#c" + seq;
                case POSITIONAL -> prop + "#p" + seq;
                // a UNION head has NO real property: its branches are
                // whole navigation chains parked in unionSpecs
                case UNION -> prop + "#u" + seq;
            };
        }
    }

    /** The head names a filter-lifted chain ({@code #fN}). */
    static boolean isFiltered(String head) {
        return JoinIdentity.of(head).kind() == JoinIdentity.Kind.FILTERED;
    }

    @com.legend.Nullable TypedLambda pred(String head) {
        return preds.get(head);
    }

    boolean hasPred(String head) {
        return preds.containsKey(head) || branchPreds.containsKey(head)
                || corrPreds.containsKey(head) || positional.containsKey(head);
    }

    /** The positional pick parked on {@code head} ({@code #pN}), or null. */
    @com.legend.Nullable Integer positionalPick(String head) {
        return positional.get(head);
    }

    /** The synthetic identity for a POSITIONAL pick over a to-many
     * navigation ({@code $t.columns->at(k)}): reused for an equal
     * (property, k) pair. Materialization filters the target to the row
     * whose store ORDINAL is k — an ordered collection's position IS data
     * (the metamodel store seeds Table.columns' declaration order); a
     * target without an ordinal walls loudly (an unordered navigation has
     * no k-th row). */
    String parkPositional(String prop, int k) {
        for (var e : positional.entrySet()) {
            if (realHead(e.getKey()).equals(prop) && e.getValue() == k) {
                return e.getKey();
            }
        }
        String synth = new JoinIdentity(prop, JoinIdentity.Kind.POSITIONAL, count++).encoded();
        positional.put(synth, k);
        return synth;
    }

    /** The k-th row of a target pipeline by the store's ORDINAL column. */
    private TypedSpec positionalRows(TypedSpec pipe, int k) {
        Type.RelationType row = Type.requireRelationSchema(pipe.info().type());
        String ord = com.legend.builtin.SystemMetamodel.ORDINAL_COLUMN;
        boolean ordered = row.columns().stream().anyMatch(c -> c.name().equals(ord));
        if (!ordered) {
            throw new com.legend.error.NotImplementedException(
                    "positional pick (at/first) over an UNORDERED navigation is not"
                    + " supported yet — only the metamodel store's ordered collections"
                    + " carry a row ordinal");
        }
        String r = "_pos";
        var one = Multiplicity.Bounded.ONE;
        TypedSpec rv = new com.legend.compiler.spec.typed.TypedVariable(r, new ExprType(row, one));
        TypedSpec read = new TypedPropertyAccess(rv, ord,
                new ExprType(Type.Primitive.INTEGER, Multiplicity.Bounded.ZERO_ONE));
        var eq = ctx.findFunction("meta::pure::functions::boolean::equal").stream()
                .filter(f -> f.parameters().size() == 2).findFirst().orElseThrow();
        TypedSpec cond = new com.legend.compiler.spec.typed.TypedNativeCall(eq, List.of(read,
                new com.legend.compiler.spec.typed.TypedCInteger((long) k,
                        new ExprType(Type.Primitive.INTEGER, one))),
                new ExprType(Type.Primitive.BOOLEAN, one));
        TypedLambda pred = new TypedLambda(List.of(r), List.of(cond),
                new ExprType(new Type.FunctionType(List.of(new Type.Param(row, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        return new com.legend.compiler.spec.typed.TypedFilter(pipe, pred, pipe.info());
    }

    /** The CORRELATED predicate parked on {@code head}, or null. */
    @com.legend.Nullable TypedLambda correlatedPred(String head) {
        return corrPreds.get(head);
    }

    /** ALL parked correlated predicates — the demand scan reads their
     * OUTER-variable paths as PARENT demand (#69: the lift moved the
     * only occurrence of the read out of the projection column). */
    java.util.Collection<TypedLambda> allCorrelatedPreds() {
        return corrPreds.values();
    }

    /** ALL predicates parked on a head: singleton for a {@code #fN} head,
     * the non-null branch predicates for a {@code #cN} head, empty
     * otherwise. Demand/tail scans iterate this — every branch's reads
     * pull the target's slots exactly like a single lifted predicate. */
    List<TypedLambda> allPreds(String head) {
        TypedLambda single = preds.get(head);
        if (single == null) {
            single = corrPreds.get(head);
        }
        if (single != null) {
            return List.of(single);
        }
        List<TypedLambda> branches = branchPreds.get(head);
        if (branches != null) {
            return branches.stream().filter(java.util.Objects::nonNull).toList();
        }
        return List.of();
    }

    /**
     * Apply a head's parked filter material to its finished target
     * pipeline: a {@code #fN} head filters once; a {@code #cN} head maps
     * each branch (a null branch predicate = the unfiltered stream) and
     * UNION-ALLs the branch pipes (engine: concatenated navigation
     * streams join as one union subselect). PLAIN/DATED heads pass
     * through.
     */
    TypedSpec applyToPipe(String head, TypedSpec pipe,
            java.util.function.BiFunction<TypedSpec, TypedLambda, TypedSpec> filter) {
        Integer k = positional.get(head);
        if (k != null) {
            return positionalRows(pipe, k);
        }
        TypedLambda single = preds.get(head);
        if (single != null) {
            return filter.apply(pipe, single);
        }
        List<TypedLambda> branches = branchPreds.get(head);
        if (branches == null || branches.isEmpty()) {
            // empty branch list = nothing parked (registration never
            // stores an empty list; the old code returned NULL here)
            return pipe;
        }
        TypedLambda b0 = branches.get(0);
        TypedSpec out = b0 == null ? pipe : filter.apply(pipe, b0);
        for (TypedLambda b : branches.subList(1, branches.size())) {
            TypedSpec member = b == null ? pipe : filter.apply(pipe, b);
            out = new com.legend.compiler.spec.typed.TypedConcatenate(
                    out, member, member.info());
        }
        return out;
    }

    /** A fresh date-fingerprinted identity for {@code prop}. */
    String mintDateName(String prop) {
        return new JoinIdentity(prop, JoinIdentity.Kind.DATED, count++).encoded();
    }

    /** A fresh filter-lifted identity for {@code prop}. */

    /** The synthetic identity for a (property, predicate) pair — REUSED
     * when an EQUAL predicate is already parked on the same real head
     * (engine merge-by-identity: employeesByCityOrManager('Hoboken','Bla')
     * twice plus its inline spelling share ONE subselect; per-call-site
     * minting over-fragments and cross-multiplies projection rows — the
     * Fork golden's 3 joins for 5 columns). Structural record equality;
     * alpha-variant spellings stay separate (safe over-fragmentation). */
    private String parkFiltered(String prop, TypedLambda pred) {
        return parkFiltered(prop, pred, false);
    }

    /** {@code valuePosition}: the head joins ROW-DROPPING (INNER — §4AD
     * P1 placement bit). Identity FORKS by placement class: a value
     * occurrence never shares a projection occurrence's join copy (one
     * join cannot be both INNER and LEFT); equal (prop, pred, placement)
     * still share one identity. */
    private String parkFiltered(String prop, TypedLambda pred,
            boolean valuePosition) {
        return parkFiltered(prop, pred, valuePosition, null);
    }

    /** {@code scope}: the ELEMENT the predicate was re-based onto
     * ({@link #rebaseToElement}) — part of the identity: two chains off
     * different heads never share one re-based predicate. */
    private String parkFiltered(String prop, TypedLambda pred,
            boolean valuePosition, @com.legend.Nullable ElementScope scope) {
        boolean closed = predClosedOverParam(pred);
        java.util.Map<String, TypedLambda> pool = closed ? preds : corrPreds;
        TypedSpec canon = alphaCanonicalBody(pred);
        for (var e : pool.entrySet()) {
            if (realHead(e.getKey()).equals(prop)
                    && alphaCanonicalBody(e.getValue()).equals(canon)
                    && innerValueHeads.contains(e.getKey()) == valuePosition
                    && java.util.Objects.equals(elementScopes.get(e.getKey()),
                            scope)) {
                return e.getKey();
            }
        }
        String synth = mintFilteredName(prop);
        pool.put(synth, pred);
        if (valuePosition) {
            innerValueHeads.add(synth);
        }
        if (scope != null) {
            elementScopes.put(synth, scope);
        }
        return synth;
    }

    /** ISOLATION (engine forced self-join — testForcedSelfJoin
     * isolationTest, batch 106): a filtered hop's correlated predicate
     * whose OUTER reads ALL pass through the chain's FIRST hop
     * ({@code $x.employees.group.children->filter(c | … ==
     * $x.employees.product.name)}) is the fan-out ELEMENT's own predicate
     * — the engine copies the element's table keyed by its PK
     * ({@code persontable_2.ID = persontable_0.ID}) and resolves the read
     * on the copy. The registry records the element the predicate was
     * re-based onto: its variable, the head property it hangs off, and
     * the element class. The application site is the HEAD'S TARGET
     * materialization (NavMaterializer's nested reroute), never the root. */
    record ElementScope(String var, String head, String classFqn) {}

    private final Map<String, ElementScope> elementScopes =
            new LinkedHashMap<>();

    @com.legend.Nullable ElementScope elementScope(String head) {
        return elementScopes.get(head);
    }

    boolean isElementScoped(String head) {
        return elementScopes.containsKey(head);
    }

    /** The predicate parked on {@code synth} was re-based onto the
     * element of {@code headProp} (real-name match: a synthetic identity
     * of the head carries the same element). */
    boolean isElementScopedTo(String synth, String headProp) {
        ElementScope sc = elementScopes.get(synth);
        return sc != null && sc.head().equals(realHead(headProp));
    }

    private record Rebased(TypedLambda pred, ElementScope scope) {}

    /** The re-based predicate for a filtered hop whose chain BELOW the
     * hop is {@code below} ({@code $x.employees.group} for {@code
     * …group.children->filter}); null when the shape does not apply: the
     * hop hangs directly off the variable (the root's own reroute serves
     * it), the predicate reads another outer variable, or any outer read
     * does not pass through the first hop (those keep their loud walls —
     * a wrong row is never a gap). */
    private @com.legend.Nullable Rebased rebaseToElement(TypedLambda pred,
            TypedSpec below) {
        if (pred.parameters().size() != 1 || pred.body().size() != 1) {
            return null;
        }
        TypedSpec cur = below;
        TypedPropertyAccess first = null;
        while (first == null) {
            TypedSpec inner = cur;
            if (inner instanceof TypedNativeCall c && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(
                            c.callee().qualifiedName())) {
                inner = c.args().get(0);
            }
            if (!(inner instanceof TypedPropertyAccess pa)) {
                return null;
            }
            if (pa.source() instanceof TypedVariable) {
                first = pa;
            } else {
                cur = pa.source();
            }
        }
        String bottom = ((TypedVariable) first.source()).name();
        TypedSpec body = pred.body().get(0);
        Set<String> free = new LinkedHashSet<>();
        readVarNames(body, free);
        free.removeAll(pred.parameters());
        if (!free.equals(Set.of(bottom))
                || !(Type.asClassType(first.info().type()) instanceof Type.ClassType ct)) {
            return null;
        }
        Set<List<String>> outer = new LinkedHashSet<>();
        FlattenOps.consumedPaths(body, bottom, outer);
        String firstProp = first.property();
        // consumedPaths reports every PREFIX of a read: judge the maximal
        // ones (the reads themselves) — each must pass through the hop
        List<List<String>> reads = outer.stream()
                .filter(p -> outer.stream().noneMatch(q -> q.size() > p.size()
                        && q.subList(0, p.size()).equals(p)))
                .toList();
        if (reads.isEmpty() || reads.stream().anyMatch(p -> p.size() < 2
                || !p.get(0).equals(firstProp))) {
            return null;
        }
        Set<String> taken = new LinkedHashSet<>(pred.parameters());
        readVarNames(body, taken);
        String var = "_el";
        for (int i = 2; taken.contains(var); i++) {
            var = "_el" + i;
        }
        TypedVariable el = new TypedVariable(var,
                new ExprType(ct, Multiplicity.Bounded.ONE));
        TypedSpec rebased = replaceFirstHop(body, bottom, first.property(), el);
        return new Rebased(
                new TypedLambda(pred.parameters(), List.of(rebased), pred.info()),
                new ElementScope(var, first.property(), ct.fqn()));
    }

    private static TypedSpec replaceFirstHop(TypedSpec n, String bottom,
            String prop, TypedVariable el) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(bottom) && pa.property().equals(prop)) {
            return el;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(bottom)) {
            return l;   // shadowed below this point
        }
        return rebuildChildren(n, c -> replaceFirstHop(c, bottom, prop, el));
    }

    /** Heads whose join is ROW-DROPPING (INNER): value-position lifts.
     * Consumed at the AssocJoin construction sites (the join-kind fact
     * rides the AssocJoin, emission reads it — never re-derived). */
    private final Set<String> innerValueHeads = new java.util.LinkedHashSet<>();

    boolean isInnerValueHead(String head) {
        return innerValueHeads.contains(head);
    }

    /** Batch 69b: a correlated filter predicate parked on a hop at index
     * &ge; {@code from} of a nav-slot chain has NO application site — the
     * parent-copy reroute's tail loop applies head and first-tail-hop
     * predicates only, and the slot spine never parks a sub-hop
     * correlated pred in-target (isolationTest:
     * {@code employees.group.children->filter(c | ... == $x.employees
     * .product.name)} answered with EVERY child; the chain could not
     * reroute because a plain path had already demanded its parent
     * alias). Wall loudly — a wrong answer is never a gap. */
    void unappliedCorrelatedWall(List<String> path, int from) {
        unappliedCorrelatedWall(path, from, false);
    }

    /** {@code parentScopedApply}: the slot spine's sub-hop joins compose
     * parent-scoped predicates (NavMaterializer.conditionFor) — those
     * heads have an application site and pass. */
    void unappliedCorrelatedWall(List<String> path, int from,
            boolean parentScopedApply) {
        for (int hi = from; hi < path.size(); hi++) {
            // an ELEMENT-scoped pred (re-based onto path[0]'s element)
            // applies inside the head's target materialization (batch 106)
            if (correlatedPred(path.get(hi)) != null
                    && !(parentScopedApply && isParentScoped(path.get(hi)))
                    && !(hi >= 1 && isElementScopedTo(path.get(hi), path.get(0)))) {
                throw new com.legend.error.NotImplementedException(
                        "correlated filter predicate on hop '"
                        + realHead(path.get(hi))
                        + "' at depth " + (hi + 1) + " of the navigation "
                        + String.join(".", path.stream()
                                .map(SyntheticHeads::realHead).toList())
                        + " has no application site yet (the parent-copy"
                        + " reroute applies head and first-tail-hop"
                        + " predicates only)");
            }
        }
    }

    /** The predicate body with its binder renamed to a canonical name —
     * the inliner alpha-freshens per call site (e, e_1, e_2 under an
     * outer shadowing scope), which defeated plain record equality (the
     * OffsetExplosion probe: 5 subselects for 2 distinct preds). */
    private static TypedSpec alphaCanonicalBody(TypedLambda pred) {
        // audit 23 B5: a shape this canonicalization cannot handle must be
        // LOUD, not silently un-canonicalized — an un-merged identity
        // cross-multiplies projection rows (the Merge golden: 7 vs 13)
        if (pred.parameters().size() != 1 || pred.body().size() != 1) {
            throw new com.legend.error.NotImplementedException(
                    "filtered-navigation predicate"
                    + " with " + pred.parameters().size() + " parameter(s)/"
                    + pred.body().size() + " statement(s) cannot join the"
                    + " merge-by-identity registry yet");
        }
        String param = pred.parameters().get(0);
        Type.FunctionType pft = pred.functionType();
        var pInfo = pft.params().size() == 1
                ? new ExprType(pft.params().get(0).type(),
                        pft.params().get(0).multiplicity())
                : null;
        if (pInfo == null) {
            throw new IllegalStateException("resolver bug: filtered-nav"
                    + " predicate info is not a 1-param FunctionType — the"
                    + " canonical binder cannot be typed");
        }
        // the canonical binder is COMPARISON-ONLY (never emitted): the
        // NUL-prefixed name cannot collide with any parseable pure
        // identifier (audit 23 — a user var literally named _canon was
        // capturable)
        return Substitution.inlineParam(pred.body().get(0), param,
                new com.legend.compiler.spec.typed.TypedVariable(
                        "\u0000canon", pInfo));
    }

    private String mintFilteredName(String prop) {
        return new JoinIdentity(prop, JoinIdentity.Kind.FILTERED, count++).encoded();
    }

    /** A fresh concatenated-stream identity for {@code prop}. */
    private String mintConcatName(String prop) {
        return new JoinIdentity(prop, JoinIdentity.Kind.CONCAT, count++).encoded();
    }

    /** Scan entry: the lambda's BODY under its own parameter (never the lambda node). */
    /**
     * PRE-REWRITE: a filtered navigation consumed as a BARE COLLECTION —
     * {@code $o.head(%d)->filter(f).leaf} with non-scalar multiplicity —
     * lifts into a SYNTHETIC head {@code head#fN}: a plain 2-hop chain
     * whose association-join TARGET pipeline carries the substituted
     * predicate (engine parity: the chain filter parks INSIDE the
     * navigation's join-tree node; the LEFT join row-explodes and
     * delivers NULL — TDSNull — on no surviving match). Scalar
     * ({@code [0..1]}) bare reads stay with the correlated-scalar arm
     * ({@code filteredNavLeafRead}) — the split is exactly complementary.
     * The walk is BEST-EFFORT: unknown node kinds pass through unchanged,
     * so an unlifted shape keeps today's loud not-substitutable error —
     * never silent SQL.
     */
    TypedSpec liftFilteredHeads(TypedSpec n) {
        return liftFilteredHeads(n, true);
    }


    /** Node-local canonicalizer applied before the lift arms (identity by
     * default) — the resolver wires the subType-cast rewrite here so a
     * witness-bearing cast becomes the filtered-nav shape THIS pass
     * already lifts (per-cast join identity via parkFiltered). */
    private java.util.function.UnaryOperator<TypedSpec> canon =
            java.util.function.UnaryOperator.identity();

    void setCanonicalizer(java.util.function.UnaryOperator<TypedSpec> c) {
        canon = c;
    }

    /** §4AD P2: filter-position conjoin channel. Inside a TypedFilter
     * predicate, a lifted filtered read parks BARE and its β-inlined
     * qualifier predicate joins {@code pending}; the wrapper attaches
     * pending conjuncts at the NEAREST boolean ancestor — the consuming
     * comparison — reproducing the engine's per-disjunct
     * (qual-pred AND cmp) grouping (testQualifierQueryWithOr cell)
     * structurally, for every operator family. */
    private record FilterCtx(List<TypedSpec> pending) {
        FilterCtx() {
            this(new java.util.ArrayList<>());
        }
    }

    private TypedSpec liftFilteredHeads(TypedSpec n, boolean enabled) {
        return liftFilteredHeads(n, enabled, null);
    }

    private TypedSpec liftFilteredHeads(TypedSpec n, boolean enabled,
            @com.legend.Nullable FilterCtx fc) {
        TypedSpec r = liftArms(n, enabled, fc);
        if (fc != null && !fc.pending().isEmpty()
                && r.info().type() == Type.Primitive.BOOLEAN
                && r.info().multiplicity()
                        instanceof Multiplicity.Bounded mb1
                && Integer.valueOf(1).equals(mb1.upper())
                && Integer.valueOf(1).equals(mb1.lower())) {
            for (int i = fc.pending().size() - 1; i >= 0; i--) {
                r = andExpr(fc.pending().get(i), r);
            }
            fc.pending().clear();
        }
        return r;
    }

    /** {@code a && b} at EXPRESSION level (andMerge's lambda-less twin). */
    private TypedSpec andExpr(TypedSpec a, TypedSpec b) {
        var fns = ctx.findFunction("meta::pure::functions::boolean::and")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected exactly"
                    + " one 2-arg boolean::and, found " + fns.size());
        }
        return new TypedNativeCall(fns.get(0), List.of(a, b), b.info());
    }

    private TypedSpec liftArms(TypedSpec n, boolean enabled,
            @com.legend.Nullable FilterCtx fc) {
        if (enabled) {
            n = canon.apply(n);
        }
        // MAP FUSION over a class collection: map(map(xs, t | f), u | $u.leaf)
        // ≡ map(xs, t | f.leaf) — pure's auto-map flattens both spellings
        // (the typer's auto-map of a derived property over a to-many
        // receiver followed by a leaf auto-map: `$b.trades
        // .productAtTimeOfTrade.name`, injection ...AutoMap). The fused
        // body is the mapper-scoped filtered-navigation shape the class-
        // mapper lift serves; unfused, the substitution's map composition
        // would splice the whole receiver chain for the element.
        if (enabled) {
            TypedSpec fused = fuseLeafOverClassMap(n);
            if (fused != null) {
                return liftFilteredHeads(fused, enabled, fc);
            }
        }
        // the instance-filter idiom is a pure rewrite (no head minted), so
        // it canonicalizes wherever it stands — inside mapper bodies too
        TypedSpec guarded = instanceFilterNavRead(n, canon);
        if (guarded != null) {
            return liftFilteredHeads(guarded, enabled, fc);
        }
        // ->map(e|$e.leaf) over a (filtered) class navigation IS the
        // property-path spelling — normalize and take the lift arm (the
        // qualifier-inlined aggregate shape:
        // joinStrings(map(filter(head, pred), .leaf)); #69).
        if (enabled && n instanceof TypedMap tm
                && tm.mapper().parameters().size() == 1
                && tm.mapper().body().size() == 1
                && tm.mapper().body().get(0) instanceof TypedPropertyAccess mb
                && mb.source() instanceof com.legend.compiler.spec.typed
                        .TypedVariable mv
                && mv.name().equals(tm.mapper().parameters().get(0))
                && filterBehindToOne(tm.source()) instanceof TypedFilter
                && Type.asClassType(tm.source().info().type()) instanceof Type.ClassType) {
            return liftFilteredHeads(new TypedPropertyAccess(
                    tm.source(), mb.property(), tm.info()), enabled, fc);
        }
        // sortBy over a FILTERED navigation (ordered sub-aggregation
        // source: filter(nav)->sortBy(key).leaf->joinStrings(...)): the
        // filter lifts into the synthetic filtered head exactly like the
        // leaf-read spelling; the sortBy rides on the renamed head as
        // ORDER metadata for the agg scan.
        if (enabled && n instanceof TypedSortBy sb0
                && sb0.source() instanceof TypedFilter fs
                && fs.predicate().parameters().size() == 1
                && Type.asClassType(fs.info().type()) instanceof Type.ClassType
                && isLiftableNav(fs.source())) {
            TypedSpec head0 = liftFilteredHeads(fs.source(), true);
            TypedSpec renamed0;
            String synth0;
            if (head0 instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma0) {
                synth0 = parkFiltered(ma0.property(), fs.predicate());
                renamed0 = new TypedMilestonedAccess(ma0.source(), synth0,
                        ma0.dates(), ma0.sweep(), ma0.info());
            } else {
                var hp0 = (TypedPropertyAccess) head0;
                synth0 = parkFiltered(hp0.property(), fs.predicate());
                renamed0 = new TypedPropertyAccess(hp0.source(), synth0,
                        hp0.info());
            }
            return new TypedSortBy(renamed0,
                    (TypedLambda) liftFilteredHeads(sb0.key(), enabled),
                    sb0.ascending(), sb0.info());
        }
        // WRAPPED filtered-nav spellings (exists-over-filter, map-wrapped
        // or stacked-filter value reads) canonicalize to the DIRECT one
        // and re-enter the walk — foldWrappedSpelling.
        if (enabled) {
            TypedSpec folded = foldWrappedSpelling(n);
            if (folded != null) {
                return liftFilteredHeads(folded, enabled, fc);
            }
        }
        TypedSpec betaLeaf = enabled ? liftMapWrappedFilterLeaf(n) : null;
        if (betaLeaf != null) {
            return betaLeaf;
        }
        if (enabled && n instanceof TypedPropertyAccess pa) {
            TypedSpec picked = liftPositionalRead(pa);
            if (picked != null) {
                return picked;
            }
        }
        if (enabled
                && n instanceof TypedPropertyAccess pa
                && filterBehindToOne(pa.source()) instanceof TypedFilter f
                && f.predicate().parameters().size() == 1
                && Type.asClassType(f.info().type())
                        instanceof Type.ClassType
                && isLiftableNav(f.source())) {
            return liftFilteredReadArm(pa, f, fc);
        }
        // COMPUTED-mapper aggregation source (#69) —
        // map(filter(nav), λe.<computed>) where the mapper body is NOT a
        // plain property read (derived-property β-inlines: concat(...)).
        // The filtered SOURCE lifts into a synthetic head exactly like the
        // leaf-read spelling; the mapper rides along and substitutes
        // through the target's bindings at the aggregation fold.
        if (enabled && n instanceof TypedMap tm2
                && tm2.mapper().parameters().size() == 1
                && tm2.source() instanceof TypedFilter f0
                && f0.predicate().parameters().size() == 1
                && Type.asClassType(f0.info().type()) instanceof Type.ClassType
                && isLiftableNav(f0.source())
                && !(tm2.info().multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded mb2
                        && Integer.valueOf(1).equals(mb2.upper()))) {
            TypedSpec head = liftFilteredHeads(f0.source(), true);
            TypedSpec renamed;
            String synth;
            if (head instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma) {
                synth = parkFiltered(ma.property(), f0.predicate());
                renamed = new TypedMilestonedAccess(
                        ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
            } else {
                var hp = (TypedPropertyAccess) head;
                synth = parkFiltered(hp.property(), f0.predicate());
                renamed = new TypedPropertyAccess(
                        hp.source(), synth, hp.info());
            }
            return new TypedMap(renamed,
                    (TypedLambda) liftFilteredHeads(tm2.mapper(), enabled),
                    tm2.info());
        }
        // BARE-AGGREGATE filtered navigation (no leaf read) — see
        // liftAggBareFilter.
        if (enabled && n instanceof TypedNativeCall agg) {
            TypedSpec lifted = liftAggBareFilter(agg, enabled);
            if (lifted != null) {
                return lifted;
            }
        }
        TypedSpec ccLift = enabled ? liftConcatArm(n) : null;
        if (ccLift != null) {
            return ccLift;
        }
        return descend(n, enabled, fc);
    }

    /** The structural descent: every node kind the lift walks through,
     * rebuilt with lifted children (unknown kinds pass unchanged). */
    private TypedSpec descend(TypedSpec n, boolean enabled,
            @com.legend.Nullable FilterCtx fc) {
        return switch (n) {
            case TypedProject p ->
                    new TypedProject(
                            liftFilteredHeads(p.source(), enabled),
                            p.columns().stream().map(c ->
                                    new TypedFuncCol(
                                            c.name(),
                                            (TypedLambda) liftFilteredHeads(c.fn(),
                                                    enabled && !valuesLambdas
                                                            .contains(c.fn()))))
                                    .toList(),
                            p.info());
            case TypedFilter f -> {
                // each predicate STATEMENT gets its own conjoin scope;
                // the statement root is Boolean[1], so the wrapper
                // attaches any conjunct the walk left pending
                FilterCtx pfc = new FilterCtx();
                TypedLambda p0 = f.predicate();
                // the INSTANCE-filter idiom over a [1] class instance
                // (`$order->filter(o | $o.product($o.orderDate).type ==
                // 'STOCK')` — the external-function spelling
                // filterOrders($o)): the predicate's parameter ALIASES the
                // instance — spell its reads on the instance itself so
                // every scan (temporal specs, slot demand, the CASE-WHEN
                // instance read) sees the instance's own paths
                if (enabled && f.source() instanceof TypedVariable iv
                        && Type.asClassType(iv.info().type()) instanceof Type.ClassType
                        && iv.info().multiplicity() instanceof Multiplicity.Bounded ib
                        && Integer.valueOf(1).equals(ib.upper())
                        && p0.parameters().size() == 1) {
                    String ip = p0.parameters().get(0);
                    p0 = new TypedLambda(p0.parameters(), p0.body().stream()
                            .map(b -> Substitution.inlineParam(b, ip, iv)).toList(),
                            p0.info());
                }
                TypedLambda p2 = new TypedLambda(p0.parameters(),
                        p0.body().stream()
                                .map(b -> liftFilteredHeads(b, enabled, pfc))
                                .toList(),
                        p0.info());
                if (!pfc.pending().isEmpty()) {
                    throw new IllegalStateException("resolver bug: "
                            + pfc.pending().size() + " filter-position"
                            + " conjunct(s) never attached to a boolean"
                            + " consumption");
                }
                yield new TypedFilter(
                        liftFilteredHeads(f.source(), enabled),
                        p2, f.info());
            }
            case TypedSortBy sb -> new TypedSortBy(
                    liftFilteredHeads(sb.source(), enabled),
                    (TypedLambda) liftFilteredHeads(sb.key(), enabled),
                    sb.ascending(), sb.keyAlias(), sb.info());
            case TypedLimit l -> new TypedLimit(
                    liftFilteredHeads(l.source(), enabled), l.count(), l.info());
            case TypedDrop d -> new TypedDrop(
                    liftFilteredHeads(d.source(), enabled), d.count(), d.info());
            case TypedSlice sl -> new TypedSlice(
                    liftFilteredHeads(sl.source(), enabled),
                    sl.start(), sl.stop(), sl.info());
            case TypedFrom fr -> new TypedFrom(
                    liftFilteredHeads(fr.source(), enabled),
                    fr.context(), fr.executedExtent(), fr.info());
            case TypedLambda l -> new TypedLambda(l.parameters(),
                    l.body().stream().map(b -> liftFilteredHeads(b, enabled))
                            .toList(), l.info());
            // AGGREGATION arguments suspend the filter-conjoin channel
            // (aggregated reads ride the GROUPED route — pred-in-
            // subselect is the measured cell; a conjunct would widen
            // the boolean leaf across agg heads: the validation
            // milestoning-aggregation trio walled on exactly that).
            // NEGATION suspends it too: the to-many negation arm
            // transcribes the engine's null-compensation for equal/in
            // — not(and(guard, cmp)) is outside its vocabulary
            // (validateComplexValidation6 walled). Negated-consumption
            // pad behavior stays the batch-7 residue (ledgered).
            case TypedNativeCall c ->
                    c.withChildren(c.args().stream()
                            .map(a -> liftFilteredHeads(a, enabled,
                                    CorrelatedSubselects.isAggregate(c)
                                            || "meta::pure::functions::boolean::not"
                                                    .equals(c.callee()
                                                            .qualifiedName())
                                            ? null : fc))
                                    .toList());
            case TypedPropertyAccess pa ->
                    new TypedPropertyAccess(
                            liftFilteredHeads(pa.source(), enabled),
                            pa.property(), pa.info());
            case TypedMilestonedAccess ma ->
                    new TypedMilestonedAccess(
                            liftFilteredHeads(ma.source(), enabled), ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
            // auto-map mapper bodies are VALUE flattenings (empties drop) —
            // the TDS lift stays off inside them; unlifted shapes keep
            // their loud error. EXCEPT a mapper over a CLASS collection
            // (an auto-mapped navigation, `$b.trades->map(t | $t.products
            // ->filter(p | $p.date == $t.d)->toOne().name)` — the
            // qualifier-inlined chained shape, injection
            // testProjectThroughAssociation): its filtered navigations off
            // the mapper's own element lift exactly like the root's, the
            // chained hop then carries the correlated predicate in its
            // ON clause (registerAssociationJoins hop>0 + associationJoin's
            // andCorrelatedIntoCondition)
            case TypedMap m -> {
                boolean classMapper = enabled && Type.asClassType(m.source().info().type())
                        instanceof Type.ClassType
                        && m.mapper().parameters().size() == 1;
                if (classMapper) {
                    mapperScope.push(m.mapper().parameters().get(0));
                }
                try {
                    yield new TypedMap(
                            liftFilteredHeads(m.source(), enabled),
                            (TypedLambda) liftFilteredHeads(m.mapper(), classMapper),
                            m.info());
                } finally {
                    if (classMapper) {
                        mapperScope.pop();
                    }
                }
            }
            case TypedIf i ->
                    new TypedIf(
                            liftFilteredHeads(i.condition(), enabled, fc),
                            liftFilteredHeads(i.thenBranch(), enabled, fc),
                            i.elseBranch().map(e ->
                                    liftFilteredHeads(e, enabled, fc)),
                            i.info());
            case TypedCollection c -> c.withChildren(c.elements().stream().map(e ->
                    liftFilteredHeads(e, enabled, fc)).toList());
            case TypedCast c ->
                    new TypedCast(
                            liftFilteredHeads(c.source(), enabled, fc),
                            c.target(), c.info(), c.wire());
            // A constructed instance is a VALUE node like a collection: the
            // lift reaches into every field (the map-over-row form's
            // ^Inst(... $r.columns->at(0).name ...) body).
            case com.legend.compiler.spec.typed.TypedNewInstance ni -> {
                java.util.Map<String, TypedSpec> ps = new java.util.LinkedHashMap<>();
                ni.properties().forEach((k, v) ->
                        ps.put(k, liftFilteredHeads(v, enabled, fc)));
                yield new com.legend.compiler.spec.typed.TypedNewInstance(ni.classFqn(), ps, ni.info());
            }
            case TypedGroupBy gb ->
                    new TypedGroupBy(
                            liftFilteredHeads(gb.source(), enabled),
                            gb.keys().stream().map(k ->
                                    new TypedGroupBy.GroupKey(k.column(),
                                            k.fn().map(fn -> (TypedLambda)
                                                    liftFilteredHeads(fn,
                                                            enabled))))
                                    .toList(),
                            gb.aggs().stream().map(a ->
                                    new TypedAggCol(a.name(), (TypedLambda)
                                            liftFilteredHeads(a.map(), enabled),
                                            a.reduce(),
                                            a.order().stream().map(o -> new TypedAggCol.AggOrder(
                                                    (TypedLambda) liftFilteredHeads(o.key(), enabled),
                                                    o.ascending(), o.nullOrder())).toList()))
                                    .toList(),
                            gb.info());
            default -> n;
        };
    }

    /**
     * BARE-AGGREGATE filtered navigation (no leaf read):
     * count(filter($p.firm->toOne().employees, pred)) — the filter is the
     * DIRECT collection argument of an aggregate call (engine:
     * employeesByAge(30)->count() groups the filtered chained hop in a
     * parent-keyed subselect). The filter lifts into a synthetic filtered
     * head exactly like the leaf-read spelling; CONTEXT-GATED to the
     * aggregate-argument position — a global bare-filter arm hijacks
     * exists-over-filter and correlated shapes owned by other routes (the
     * reverted -22 regression). Null when the arm does not apply.
     */
    /** THE filtered-read arm (§4AD batches 5+7 + P2): every
     * filtered-nav read takes the fan-out route, ALL positions and
     * multiplicities (charter decisions 1-2; batch-0 placement table —
     * no position gates). CLOSED predicates park on the target
     * pipeline; a predicate reading the OUTER row parks CORRELATED
     * (applied at the join condition); EQUAL preds on one head REUSE
     * one identity (parkFiltered). §4AD P2 — FILTER position: the
     * IN-TARGET park stays (the engine's own emission for these
     * shapes is pred-in-ON — nestedFilterFunctionExpressionWithOr-
     * Condition golden — and its fan counts are the measured rows),
     * PLUS the inlined qualifier pred conjoins the consuming
     * comparison via the pending channel: REDUNDANT over matched
     * rows, the PAD GUARD over unmatched ones — a pad row's NULL
     * reads could otherwise satisfy a null-safe comparison the
     * qual-pred should have guarded (both engine forms drop that
     * row — ValueMapPlacementTest.filterPositionGroupsQualPredWithCmp). */
    /** A leaf read through a POSITIONAL pick over a bare to-many
     * navigation head — {@code $t.columns->at(k)[->cast(@C)].name} — lifts
     * into the synthetic head {@code columns#pN} (a to-one read: the k-th
     * row by the store ordinal); null for any other shape. */
    private @com.legend.Nullable TypedSpec liftPositionalRead(TypedPropertyAccess pa) {
        TypedSpec src = pa.source();
        Type castTo = null;
        while (true) {
            if (src instanceof com.legend.compiler.spec.typed.TypedCast tc) {
                castTo = castTo == null ? tc.target() : castTo;
                src = tc.source();
                continue;
            }
            if (src instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                src = c.args().get(0);
                continue;
            }
            break;
        }
        if (!(src instanceof com.legend.compiler.spec.typed.TypedNativeCall at)
                || !Anchors.isStaticAt(at)
                || !(at.args().get(0) instanceof TypedPropertyAccess nav)
                || !isLiftableNav(nav)
                || !(Type.asClassType(nav.info().type()) instanceof Type.ClassType)
                || !(nav.info().multiplicity() instanceof Multiplicity.Bounded nb) || !nb.isMany()) {
            return null;
        }
        int k = (int) ((com.legend.compiler.spec.typed.TypedCInteger) at.args().get(1)).value().longValue();
        Type headType = castTo != null && Type.asClassType(castTo) instanceof Type.ClassType
                ? castTo : nav.info().type();
        TypedSpec renamed = new TypedPropertyAccess(nav.source(),
                parkPositional(nav.property(), k),
                new ExprType(headType, Multiplicity.Bounded.ZERO_ONE));
        return new TypedPropertyAccess(renamed, pa.property(), pa.info());
    }

    /** THE instance-filter idiom — a read through a filter over the [1]
     * instance itself, {@code filter($r, pred).leaf} (engine golden
     * testConcatenateWithFilter: CASE WHEN pred THEN leaf ELSE NULL) and
     * its navigation form {@code filter($r, pred).hop.leaf} (the
     * subtype-cast canon's spelling of {@code $r->subType(@Bicycle)
     * .person.name}: the member witness as the filter, the cast's slot as
     * the hop) — is {@code if(pred[$r], | $r.hops.leaf, | [])}, spelled so
     * every scan demands the hops on the instance and the substitution's
     * plain arms serve the reads. One owner for every hop count. */
    private static @com.legend.Nullable TypedSpec instanceFilterNavRead(TypedSpec n,
            java.util.function.UnaryOperator<TypedSpec> canon) {
        List<TypedPropertyAccess> hops = new java.util.ArrayList<>();
        TypedSpec cur = n;
        while (cur instanceof TypedPropertyAccess pa) {
            hops.add(0, pa);
            cur = pa.source();
        }
        // the canonicalizer is node-local and top-down: the cast beneath
        // the innermost hop has not been visited yet — canonicalize that
        // hop first ($r->subType(@Bicycle).person -> filter($r, witness)
        // .stc_Bicycle___person) so the filter form is what this arm reads
        if (hops.size() >= 2 && !(cur instanceof TypedFilter)) {
            TypedSpec c0 = canon.apply(hops.get(0));
            if (c0 instanceof TypedPropertyAccess cp && cp.source() instanceof TypedFilter) {
                hops.set(0, cp);
                cur = cp.source();
            }
        }
        if (hops.isEmpty()
                || !(cur instanceof TypedFilter f)
                || !(f.source() instanceof TypedVariable iv)
                || !(Type.asClassType(iv.info().type()) instanceof Type.ClassType)
                || !(iv.info().multiplicity() instanceof Multiplicity.Bounded ib
                        && Integer.valueOf(1).equals(ib.upper()))
                || f.predicate().parameters().size() != 1
                || f.predicate().body().size() != 1) {
            return null;
        }
        TypedSpec chain = iv;
        for (TypedPropertyAccess h : hops) {
            chain = new TypedPropertyAccess(chain, h.property(), h.info());
        }
        TypedSpec cond = Substitution.inlineParam(f.predicate().body().get(0),
                f.predicate().parameters().get(0), iv);
        return new com.legend.compiler.spec.typed.TypedIf(cond, chain,
                java.util.Optional.empty(),
                new ExprType(n.info().type(), Multiplicity.Bounded.ZERO_ONE));
    }

    /** The two spellings of a leaf read over a class-collection map —
     * {@code map(map(xs, t | f), u | $u.leaf)} and the auto-map sugar
     * {@code map(xs, t | f).leaf} — fused to {@code map(xs, t | f.leaf)}
     * when {@code f} is class-typed. Null when not that shape. */
    private static @com.legend.Nullable TypedSpec fuseLeafOverClassMap(TypedSpec n) {
        TypedMap inner;
        String leaf;
        ExprType leafInfo;
        ExprType outInfo;
        if (n instanceof TypedMap outer
                && outer.source() instanceof TypedMap im
                && outer.mapper().parameters().size() == 1
                && outer.mapper().body().size() == 1
                && outer.mapper().body().get(0) instanceof TypedPropertyAccess ob
                && ob.source() instanceof TypedVariable ov
                && ov.name().equals(outer.mapper().parameters().get(0))) {
            inner = im;
            leaf = ob.property();
            leafInfo = ob.info();
            outInfo = outer.info();
        } else if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedMap im2) {
            inner = im2;
            leaf = pa.property();
            leafInfo = new ExprType(pa.info().type(), Multiplicity.Bounded.ZERO_ONE);
            outInfo = pa.info();
        } else {
            return null;
        }
        if (!(Type.asClassType(inner.source().info().type()) instanceof Type.ClassType)
                || inner.mapper().parameters().size() != 1
                || inner.mapper().body().size() != 1
                || !(Type.asClassType(inner.mapper().body().get(0).info().type()) instanceof Type.ClassType)) {
            return null;
        }
        TypedSpec f = inner.mapper().body().get(0);
        var ft = inner.mapper().functionType();
        TypedLambda fused = new TypedLambda(inner.mapper().parameters(),
                List.of(new TypedPropertyAccess(f, leaf, leafInfo)),
                new ExprType(new Type.FunctionType(ft.params(),
                        new Type.Param(leafInfo.type(), leafInfo.multiplicity())),
                        Multiplicity.Bounded.ONE));
        return new TypedMap(inner.source(), fused, outInfo);
    }

    private TypedSpec liftFilteredReadArm(TypedPropertyAccess pa,
            TypedFilter f, @com.legend.Nullable FilterCtx fc) {
        if (fc != null && f.predicate().body().size() == 1) {
            TypedSpec headF = liftFilteredHeads(f.source(), true, fc);
            TypedSpec renamedF;
            if (headF instanceof TypedMilestonedAccess maF) {
                renamedF = new TypedMilestonedAccess(maF.source(),
                        parkFiltered(maF.property(), f.predicate()),
                        maF.dates(), maF.sweep(), maF.info());
            } else {
                var hpF = (TypedPropertyAccess) headF;
                renamedF = new TypedPropertyAccess(hpF.source(),
                        parkFiltered(hpF.property(), f.predicate()),
                        hpF.info());
            }
            fc.pending().add(Substitution.inlineParam(
                    f.predicate().body().get(0),
                    f.predicate().parameters().get(0), renamedF));
            return new TypedPropertyAccess(renamedF, pa.property(),
                    pa.info());
        }
        TypedSpec head = liftFilteredHeads(f.source(), true);
        TypedSpec renamed;
        String synth;
        if (head instanceof com.legend.compiler.spec.typed
                .TypedMilestonedAccess ma) {
            synth = parkFiltered(ma.property(), f.predicate());
            renamed = new TypedMilestonedAccess(ma.source(), synth,
                    ma.dates(), ma.sweep(), ma.info());
        } else {
            var hp = (TypedPropertyAccess) head;
            // ISOLATION: an outer read that passes through the chain's
            // first hop is the fan-out element's own — re-base (batch 106)
            Rebased rb = rebaseToElement(f.predicate(), hp.source());
            synth = rb == null ? parkFiltered(hp.property(), f.predicate())
                    : parkFiltered(hp.property(), rb.pred(), false, rb.scope());
            renamed = new TypedPropertyAccess(hp.source(), synth, hp.info());
        }
        markParentScoped(synth, f);
        return new TypedPropertyAccess(renamed, pa.property(), pa.info());
    }

    /** The lift is walking a mapper body over a CLASS collection: the
     * mapper's element variable, innermost first. */
    private final java.util.ArrayDeque<String> mapperScope = new java.util.ArrayDeque<>();

    /** Heads minted INSIDE a class-collection mapper whose filtered
     * navigation hangs directly off the mapper's element and whose
     * correlated predicate reads ONLY that element: the predicate's outer
     * reads are the PARENT hop's own row — the sub-hop join's ON clause
     * is its application site (NavMaterializer's conditionFor). Any other
     * correlated sub-hop predicate keeps the unapplied wall. */
    private final Set<String> parentScopedHeads = new java.util.LinkedHashSet<>();

    boolean isParentScoped(String head) {
        return parentScopedHeads.contains(head);
    }

    private void markParentScoped(String synth, TypedFilter f) {
        String scope = mapperScope.peek();
        if (scope == null || !corrPreds.containsKey(synth)
                || !(f.source() instanceof TypedPropertyAccess src0)
                || !(src0.source() instanceof TypedVariable v0)
                || !v0.name().equals(scope)) {
            return;
        }
        TypedLambda pred = f.predicate();
        Set<String> reads = new java.util.LinkedHashSet<>();
        for (TypedSpec b : pred.body()) {
            readVarNames(b, reads);
        }
        reads.removeAll(pred.parameters());
        if (reads.equals(Set.of(scope))) {
            parentScopedHeads.add(synth);
        }
    }

    private static void readVarNames(TypedSpec n, Set<String> out) {
        if (n instanceof TypedVariable v) {
            out.add(v.name());
        }
        if (n instanceof TypedLambda l) {
            Set<String> inner = new java.util.LinkedHashSet<>();
            for (TypedSpec b : l.body()) {
                readVarNames(b, inner);
            }
            inner.removeAll(l.parameters());
            out.addAll(inner);
            return;
        }
        for (TypedSpec c : n.children()) {
            readVarNames(c, out);
        }
    }

    private @com.legend.Nullable TypedSpec liftAggBareFilter(
            TypedNativeCall agg, boolean enabled) {
        if (agg.args().isEmpty()
                || !CorrelatedSubselects.isAggregate(agg)
                || !(agg.args().get(0) instanceof TypedFilter fa)
                || fa.predicate().parameters().size() != 1
                || !(Type.asClassType(fa.info().type()) instanceof Type.ClassType)
                || !isLiftableNav(fa.source())
                || (fa.info().multiplicity()
                        instanceof Multiplicity.Bounded ab
                        && Integer.valueOf(1).equals(ab.upper()))) {
            return null;
        }
        TypedSpec head = liftFilteredHeads(fa.source(), true);
        TypedSpec renamed;
        String synth;
        if (head instanceof TypedMilestonedAccess ma) {
            synth = parkFiltered(ma.property(), fa.predicate());
            renamed = new TypedMilestonedAccess(
                    ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
        } else {
            var hp = (TypedPropertyAccess) head;
            synth = parkFiltered(hp.property(), fa.predicate());
            renamed = new TypedPropertyAccess(
                    hp.source(), synth, hp.info());
        }
        List<TypedSpec> newArgs = new java.util.ArrayList<>(agg.args());
        newArgs.set(0, renamed);
        for (int i = 1; i < newArgs.size(); i++) {
            newArgs.set(i, liftFilteredHeads(newArgs.get(i), enabled));
        }
        return agg.withChildren(newArgs);
    }

    /**
     * VALUES-position filtered navigation (map terminal): pure flattening
     * DROPS empties here, so the predicate parks in the OUTER where
     * (engine golden: plain LEFT JOIN + WHERE — non-matching parents
     * contribute nothing, never a NULL value). The head still lifts to a
     * synthetic chain for join identity, but WITHOUT the in-target
     * predicate; the predicate joins the chain as an injected
     * object-space filter whose reads inline through the synthetic head.
     */
    TypedMap liftValueMapFilter(
            TypedMap m) {
        TypedLambda mapper = m.mapper();
        if (mapper.parameters().size() != 1) {
            return m;
        }
        // §4AD P1 (placement addendum §6): COMPUTED mapper bodies walk
        // too — the old single-plain-read guard was the defect boundary
        // (computed bodies fell through to the PROJECT route and
        // inherited its row-PRESERVING placement; null-skipping
        // operators then minted phantom values —
        // testQualifierWithOperation). Value heads park their predicate
        // IN-TARGET like every position (one mechanism) and differ ONLY
        // by join kind: INNER, the row-dropping placement bit. INNER
        // beats the engine's LEFT+top-WHERE hoist on the unmeasured
        // null-safe-pred cell (a hoisted null-safe pred is TRUE over
        // the LEFT pad row and mints phantoms —
        // ValueMapPlacementTest.doubleNullConjunctRuleParity caught it);
        // with INNER the pad row never exists. Row-identical to the
        // engine's emission on every measured cell.
        // THE JOIN KIND FOLLOWS THE MAPPER BODY'S PER-PARENT MULTIPLICITY
        // (batch 70, user-ratified 2026-09-05): a body that IS the
        // flattened navigation ([*]) drops non-matching parents by pure's
        // own flattening — INNER, row-identical. A body that REDUCES a
        // BARE many-valued read to one value per parent (plus over
        // String[*], joinStrings, an aggregate: `$f.employees->filter(..)
        // .firstName + 'Test'`) keeps every parent — pure's plus over an
        // empty operand is 'Test', one value per firm — so that head joins
        // LEFT with its predicate in-target. A read NARROWED by toOne()
        // stays INNER (liftValueRead): pure has no answer for an empty
        // toOne (a runtime error), so the engine's measured default cell
        // is the only spec — the forced-isolation goldens are the engine's
        // OTHER convention for that undefined case, a named decision.
        boolean reduces = mapper.body().get(mapper.body().size() - 1)
                .info().multiplicity() instanceof Multiplicity.Bounded rb
                && Integer.valueOf(1).equals(rb.upper());
        boolean[] lifted = {false};
        List<TypedSpec> body2 = new java.util.ArrayList<>(mapper.body().size());
        for (TypedSpec b : mapper.body()) {
            body2.add(liftValueRead(b, mapper, lifted, !reduces));
        }
        if (!lifted[0]) {
            return m;
        }
        TypedLambda mapper2 = new TypedLambda(mapper.parameters(), body2,
                mapper.info());
        valuesLambdas.add(mapper2);
        return new TypedMap(m.source(), mapper2, m.info());
    }

    /** One VALUE-position filtered read — {@code $p.prop->filter(pred)
     * .leaf} anywhere in the mapper body (computed bodies recurse;
     * NESTED lambdas keep their own routes — their binders are not the
     * mapper's row). toOne/first/head conformance wrappers are
     * SQL-erased (charter decision 1, same policy as the projection arm
     * and liftConcatStreams) — this arm IS the task-#72 retirement path
     * for value position. Multi-occurrence: equal preds share one
     * identity, different preds fork copies (engine golden
     * testTwoQualifiersWithOperation: persontable_0 vs persontable_2);
     * every occurrence's INNER join must match — the measured
     * ALL-preds-AND-one-WHERE row behavior, by composition. */
    private TypedSpec liftValueRead(TypedSpec n, TypedLambda mapper,
            boolean[] lifted, boolean rowDropping) {
        if (n instanceof TypedLambda) {
            return n;
        }
        if (n instanceof TypedPropertyAccess pa
                && filterBehindToOne(pa.source()) instanceof TypedFilter f
                && f.predicate().parameters().size() == 1
                && f.predicate().body().size() == 1
                && Type.asClassType(f.info().type()) instanceof Type.ClassType
                && isLiftableNav(f.source())
                && mapper.parameters().get(0).equals(bottomVarOf(f.source()))) {
            // a `->toOne()`/`->first()` NARROWED read keeps the row-dropping
            // join whatever the body does: pure has NO answer for an empty
            // toOne (a runtime error), so the engine's measured cell is the
            // only spec there (ValueMapPlacementTest pins); only a BARE
            // many-valued read reduced by the body (`.firstName + 'Test'`)
            // has pure's one-value-per-parent answer and joins LEFT
            boolean dropping = rowDropping || !(pa.source() instanceof TypedFilter);
            TypedSpec renamed;
            if (f.source() instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma) {
                renamed = new TypedMilestonedAccess(
                        ma.source(),
                        parkFiltered(ma.property(), f.predicate(), dropping),
                        ma.dates(), ma.sweep(), ma.info());
            } else {
                var hp = (TypedPropertyAccess) f.source();
                renamed = new TypedPropertyAccess(
                        hp.source(),
                        parkFiltered(hp.property(), f.predicate(), dropping),
                        hp.info());
            }
            lifted[0] = true;
            return new TypedPropertyAccess(renamed, pa.property(), pa.info());
        }
        return rebuildChildren(n, c -> liftValueRead(c, mapper, lifted, rowDropping));
    }

    /**
     * The concat-stream lift body: flatten nested binary concatenates,
     * require every branch to be a (filtered) navigation of ONE shared
     * head property bottoming at the same receiver shape, mint the
     * {@code #cN} identity and park the branch predicates in order
     * (null = unfiltered branch). Null when any branch refuses — the
     * caller falls through to the loud wall.
     */
    private @com.legend.Nullable TypedSpec liftConcatStreams(TypedNativeCall cc,
            TypedPropertyAccess leafRead) {
        List<TypedSpec> streams = new java.util.ArrayList<>();
        flattenConcat(cc, streams);
        String prop = null;
        TypedSpec headNode = null;
        List<TypedLambda> branches = new java.util.ArrayList<>(streams.size());
        for (TypedSpec s : streams) {
            TypedSpec nav;
            TypedLambda pred;
            // conform-by-emission wrappers are SQL-erased (Scalars toOne
            // policy): a derived property declared [1] over a filtered
            // stream arrives as toOne(filter(...)) — look through
            while (s instanceof TypedNativeCall w
                    && w.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())) {
                s = w.args().get(0);
            }
            if (s instanceof TypedFilter f
                    && f.predicate().parameters().size() == 1
                    && Type.asClassType(f.info().type()) instanceof Type.ClassType
                    && isLiftableNav(f.source())
                    && predClosedOverParam(f.predicate())) {
                nav = f.source();
                pred = f.predicate();
            } else if ((s instanceof TypedPropertyAccess
                    || s instanceof TypedMilestonedAccess)
                    && Type.asClassType(s.info().type()) instanceof Type.ClassType
                    && isLiftableNav(s)) {
                nav = s;
                pred = null;
            } else {
                return null;
            }
            String p = nav instanceof TypedMilestonedAccess ma
                    ? ma.property() : ((TypedPropertyAccess) nav).property();
            if (prop == null) {
                prop = p;
                headNode = nav;
            } else if (!prop.equals(p) || !nav.equals(headNode)) {
                // ONE head means one WHOLE navigation node: the property
                // AND its receiver chain AND its milestoning dates (audit
                // 16: branch 2's $p.parent hop or a different business
                // date silently vanished into branch 1's head — wrong
                // rows). Cross-head/cross-date unions are their own rung;
                // the refusal keeps the loud not-substitutable wall.
                return null;
            }
            branches.add(pred);
        }
        if (prop == null || branches.size() < 2) {
            return null;
        }
        // ONE identity per distinct stream expression: the same
        // concatenated stream in two projection columns rides ONE join
        // (engine merge-by-identity — two-column Merge golden expects 7
        // rows, two joins gave 13). The HEAD NODE is part of the identity
        // (same property over different receivers/dates is a different
        // stream); its BOTTOM VARIABLE alpha-normalizes so per-column
        // lambda param names (p| vs t|) don't split one stream into two
        // joins.
        Map<String, String> rootEnv = new LinkedHashMap<>();
        rootEnv.put(bottomVarOf(headNode), "#root");
        List<Object> memoKey = List.of(prop,
                alphaNormalize(headNode, rootEnv, new int[]{0}),
                branches.stream().map(b -> b == null ? ""
                        : (Object) canonicalPred(b)).toList());
        String synth = concatMemo.get(memoKey);
        if (synth == null) {
            synth = mintConcatName(prop);
            concatMemo.put(memoKey, synth);
            branchPreds.put(synth, branches);
        }
        TypedSpec renamed;
        if (headNode instanceof TypedMilestonedAccess ma) {
            renamed = new TypedMilestonedAccess(
                    ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
        } else {
            var hp = (TypedPropertyAccess) java.util.Objects.requireNonNull(headNode, "headNode");
            renamed = new TypedPropertyAccess(
                    java.util.Objects.requireNonNull(hp, "hp").source(), synth, hp.info());
        }
        return new TypedPropertyAccess(
                renamed, leafRead.property(), leafRead.info());
    }

    private static void flattenConcat(TypedSpec n, List<TypedSpec> out) {
        if (n instanceof TypedNativeCall c
                && c.callee().qualifiedName()
                        .equals("meta::pure::functions::collection::concatenate")
                && c.args().size() == 2) {
            flattenConcat(c.args().get(0), out);
            flattenConcat(c.args().get(1), out);
            return;
        }
        out.add(n);
    }

    /** The predicate reads no variables beyond its own parameter and the
     * parameters of lambdas that lexically ENCLOSE the read — SHADOW-AWARE
     * (audit 21b F4): a nested lambda's param binds only within that
     * lambda's subtree. An outer variable that merely shares a param's
     * name stays FREE, so a correlated pred can never look closed by name
     * collision and get applied inside the target pipeline where the
     * outer row does not exist. (Conservative the other way stays fine:
     * over-refusing the lift is loud.) */
    private static boolean predClosedOverParam(TypedLambda pred) {
        Set<String> bound = new LinkedHashSet<>(pred.parameters());
        return pred.body().stream().allMatch(b -> readsOnly(b, bound));
    }

    private static boolean readsOnly(TypedSpec n, Set<String> allowed) {
        if (n instanceof TypedVariable v
                && !allowed.contains(v.name())) {
            return false;
        }
        if (n instanceof TypedLambda l) {
            Set<String> inner = new LinkedHashSet<>(allowed);
            inner.addAll(l.parameters());
            return l.body().stream().allMatch(b -> readsOnly(b, inner));
        }
        for (TypedSpec c : n.children()) {
            if (!readsOnly(c, allowed)) {
                return false;
            }
        }
        return true;
    }

    /** The filter's source is a navigation hop whose receiver chain bottoms
     * at a lambda variable — the shape the lift can rename. */
    /** MAP-WRAPPED filtered nav over a TO-ONE receiver
     * ({@code $p.firm->map(f|$f.address->filter(corr)).name}): map over
     * [0..1]/[1] IS direct application with empty propagation (pure), and
     * a navigation body propagates null — β-inline the mapper so the leaf
     * read lands on the filter and the leaf-read arm lifts the DIRECT
     * spelling (the exploding-sub machinery). Null when not this shape. */
    private @com.legend.Nullable TypedSpec liftMapWrappedFilterLeaf(
            TypedSpec n) {
        if (n instanceof TypedPropertyAccess paM
                && paM.source() instanceof TypedMap mw
                && Type.asClassType(mw.source().info().type()) instanceof Type.ClassType
                && mw.source().info().multiplicity()
                        instanceof Multiplicity.Bounded mwb
                && Integer.valueOf(1).equals(mwb.upper())
                && mw.mapper().parameters().size() == 1
                && mw.mapper().body().size() == 1
                && mw.mapper().body().get(0) instanceof TypedFilter) {
            TypedSpec inlined = Substitution.inlineParam(
                    mw.mapper().body().get(0),
                    mw.mapper().parameters().get(0), mw.source());
            return liftFilteredHeads(new TypedPropertyAccess(
                    inlined, paM.property(), paM.info()), true);
        }
        return null;
    }

    /** CONCATENATED navigation streams read as a bare collection —
     * {@code $p.head->filter(f1).leaf} spelled over concatenate(...):
     * every branch is a (possibly filtered) navigation of the SAME head
     * property; the union lifts into ONE synthetic head #cN whose join
     * target is the UNION ALL of the branch pipelines (engine: one
     * unionalias subselect, LEFT-joined, row-exploding). Null = not
     * this shape. */
    private @com.legend.Nullable TypedSpec liftConcatArm(TypedSpec n) {
        if (!(n instanceof TypedPropertyAccess pa2)) {
            return null;
        }
        if (pa2.source() instanceof TypedNativeCall cc
                && isConcatCall(cc)
                && Type.asClassType(cc.info().type()) instanceof Type.ClassType
                && !(pa2.info().multiplicity()
                        instanceof Multiplicity.Bounded b2
                        && Integer.valueOf(1).equals(b2.upper()))) {
            TypedSpec sameHead = liftConcatStreams(cc, pa2);
            if (sameHead != null) {
                return sameHead;
            }
        }
        // CROSS-HEAD branches (engine processConcatenate, pureToSQLQuery
        // .pure:2709 + buildConcatenateSubSelect :2889): every branch a
        // whole navigation chain off the same variable through DIFFERENT
        // head properties — `$t.subAccount.oe->concatenate($t.otherAccount
        // .oe)->toOne().name`. The engine joins ONE `unionalias_N`
        // subselect (the branch chains UNION ALL-ed with their join keys
        // null-padded) on the OR of the branch conditions; the leaf reads
        // the union's shared column. A ->toOne() wrapper is SQL-erased
        // exactly as the filtered-nav lift erases it.
        TypedSpec bare = filterBehindToOne(pa2.source());
        if (bare instanceof TypedNativeCall cc2 && isConcatCall(cc2)
                && Type.asClassType(cc2.info().type()) instanceof Type.ClassType leafClass) {
            return liftUnionHead(cc2, leafClass, pa2);
        }
        return null;
    }

    private static boolean isConcatCall(TypedNativeCall c) {
        return c.callee().qualifiedName()
                .equals("meta::pure::functions::collection::concatenate");
    }

    /** The parked material of a {@code #uN} head: the leaf class and the
     * ORDERED branch paths (each a navigation chain off the head's
     * variable, hop names in order). */
    record UnionSpec(String classFqn, List<List<String>> paths) {}

    static boolean isUnion(String head) {
        return JoinIdentity.of(head).kind() == JoinIdentity.Kind.UNION;
    }

    UnionSpec unionSpec(String head) {
        return java.util.Objects.requireNonNull(unionSpecs.get(head),
                () -> "resolver bug: no union material parked on " + head);
    }

    /** The union-head lift body: every branch (through ->toOne()/first/
     * head wrappers) must be a PLAIN navigation chain of the leaf class
     * bottoming at ONE variable, and at least two DISTINCT head
     * properties must occur (a same-head concatenate is the {@code #cN}
     * stream lift's shape, never this one). Null = not this shape — the
     * caller falls through to the loud wall. Equal (variable, class,
     * branches) share ONE identity (engine merge-by-identity: the same
     * concatenated stream in two columns rides one union join). */
    private @com.legend.Nullable TypedSpec liftUnionHead(TypedNativeCall cc,
            Type.ClassType leafClass, TypedPropertyAccess leafRead) {
        List<TypedSpec> streams = new java.util.ArrayList<>();
        flattenConcat(cc, streams);
        List<List<String>> paths = new java.util.ArrayList<>(streams.size());
        Set<String> heads = new java.util.LinkedHashSet<>();
        TypedVariable bottom = null;
        for (TypedSpec s0 : streams) {
            TypedSpec s = filterBehindToOne(s0);
            if (!(Type.asClassType(s.info().type()) instanceof Type.ClassType)) {
                return null;
            }
            List<String> path = new java.util.ArrayList<>();
            TypedSpec cur = s;
            while (cur instanceof TypedPropertyAccess pa) {
                path.add(0, pa.property());
                cur = pa.source();
            }
            if (!(cur instanceof TypedVariable v) || path.isEmpty()) {
                return null;
            }
            if (bottom == null) {
                bottom = v;
            } else if (!bottom.name().equals(v.name())) {
                return null;
            }
            paths.add(path);
            heads.add(path.get(0));
        }
        if (bottom == null || paths.size() < 2 || heads.size() < 2) {
            return null;
        }
        List<Object> memoKey = List.of(bottom.name(), leafClass.fqn(), paths);
        String synth = unionMemo.get(memoKey);
        if (synth == null) {
            synth = new JoinIdentity("", JoinIdentity.Kind.UNION, count++).encoded();
            unionMemo.put(memoKey, synth);
            unionSpecs.put(synth, new UnionSpec(leafClass.fqn(), paths));
        }
        return new TypedPropertyAccess(
                new TypedPropertyAccess(bottom, synth, cc.info()),
                leafRead.property(), leafRead.info());
    }

    private static boolean isLiftableNav(TypedSpec n) {
        if (n instanceof TypedPropertyAccess pa) {
            return navBottomsAtVar(pa.source());
        }
        if (n instanceof TypedMilestonedAccess ma) {
            return navBottomsAtVar(ma.source());
        }
        return false;
    }

    /** The filter node, looking through MULTIPLICITY wrappers — a
     * {@code ->toOne()} coercion, and {@code ->first()}/{@code ->head()}
     * (a qualifier body's own narrowing; batch 5): the engine compiles
     * first-over-filtered-nav as the PLAIN fanned join with the
     * predicate in the frame (conditionRightTableNested golden — no
     * LIMIT), so under charter decision 1 the wrappers are SQL-erased
     * here exactly like toOne; the read semantics are the LEFT join's
     * NULL-on-no-match either way. Wrappers STACK (toOne(first(...))). */
    private static TypedSpec filterBehindToOne(TypedSpec n) {
        while (n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(
                        c.callee().qualifiedName())
                    || c.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first")
                    || c.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::head"))) {
            n = c.args().get(0);
        }
        return n;
    }

    /**
     * ONE-STEP canonicalization of a WRAPPED filtered-nav spelling to the
     * direct one the walk re-enters on — or {@code null} (no arm fires).
     * <ul>
     *   <li>exists over a FILTERED navigation folds the filter into the
     *       exists predicate — {@code exists(filter(X,p1),p2) ≡
     *       exists(X, p1 && p2)} (the qualifier-inlined spelling
     *       {@code $p.firm->toOne().emplByAge(30)->exists(pred)});</li>
     *   <li>WRAPPED value reads ({@code $f->map(f|$f.qual(...))
     *       ->filter(p2)->toOne().leaf}) canonicalize via
     *       {@link #canonNavChain} — the demand scan, the lift arms and
     *       the correlated-scalar arm then all see the one spelling they
     *       already handle; the TOP toOne wrapper is preserved (it marks
     *       the scalar first-row read).</li>
     * </ul>
     */
    private @com.legend.Nullable TypedSpec foldWrappedSpelling(TypedSpec n) {
        if (n instanceof TypedNativeCall ex
                && ex.callee().qualifiedName()
                        .equals("meta::pure::functions::collection::exists")
                && ex.args().size() == 2
                && filterBehindToOne(ex.args().get(0)) instanceof TypedFilter fx
                && fx.predicate().parameters().size() == 1
                && fx.predicate().body().size() == 1
                && Type.asClassType(fx.info().type()) instanceof Type.ClassType
                && ex.args().get(1) instanceof TypedLambda exp
                && exp.parameters().size() == 1
                && exp.body().size() == 1) {
            return ex.withChildren(List.of(fx.source(), andMerge(fx.predicate(), exp)));
        }
        if (n instanceof TypedPropertyAccess paw
                && !(filterBehindToOne(paw.source())
                        instanceof TypedFilter fw
                        && isLiftableNav(fw.source()))) {
            TypedSpec un = filterBehindToOne(paw.source());
            TypedSpec canon = canonNavChain(un);
            if (canon != un && canon instanceof TypedFilter) {
                TypedSpec rewrapped = un == paw.source() ? canon
                        : new TypedNativeCall(
                                ((TypedNativeCall) paw.source()).callee(),
                                List.of(canon), paw.source().info());
                return new TypedPropertyAccess(rewrapped,
                        paw.property(), paw.info());
            }
        }
        return null;
    }

    /**
     * Canonicalize a WRAPPED filtered-navigation chain to the direct
     * spelling every downstream consumer (demand scan, this lift, the
     * correlated-scalar arm) already recognizes: β-reduce a map over ONE
     * instance ({@code $f->map(f|...)} — identity plumbing over a [1]
     * receiver), look through multiplicity-only {@code toOne} coercions,
     * and collapse stacked filters into ONE AND-merged predicate —
     * {@code filter(filter(nav,p1),p2) ≡ filter(nav, p1 && p2)}, and two
     * parks would mint two synthetic heads and cross-join the target.
     * Non-lift shapes return unchanged (identity — callers compare).
     */
    private TypedSpec canonNavChain(TypedSpec s) {
        TypedSpec u = filterBehindToOne(s);
        if (u != s) {
            return canonNavChain(u);
        }
        if (s instanceof TypedMap m && m.source() instanceof TypedVariable v
                && Type.asClassType(m.source().info().type()) instanceof Type.ClassType
                && m.source().info().multiplicity()
                        instanceof Multiplicity.Bounded mb
                && Integer.valueOf(1).equals(mb.upper())
                && m.mapper().parameters().size() == 1
                && m.mapper().body().size() == 1) {
            String p = m.mapper().parameters().get(0);
            TypedSpec b = m.mapper().body().get(0);
            return canonNavChain(p.equals(v.name()) ? b
                    : Pipelines.rewriteRowReads(b, p, Map.of(), Set.of(),
                            vv -> new TypedVariable(v.name(), vv.info())));
        }
        if (s instanceof TypedFilter f
                && f.predicate().parameters().size() == 1
                && f.predicate().body().size() == 1) {
            TypedSpec src = canonNavChain(f.source());
            if (src instanceof TypedFilter inner
                    && inner.predicate().parameters().size() == 1
                    && inner.predicate().body().size() == 1) {
                return new TypedFilter(inner.source(),
                        andMerge(inner.predicate(), f.predicate()), f.info());
            }
            return src == f.source() ? s
                    : new TypedFilter(src, f.predicate(), f.info());
        }
        return s;
    }

    /** {@code λv. p1(v) && p2(v)} — alpha-aligned to p1's binder; the
     * merged predicate parks as ONE synthetic-head identity. */
    private TypedLambda andMerge(TypedLambda p1, TypedLambda p2) {
        var fns = ctx.findFunction("meta::pure::functions::boolean::and")
                .stream().filter(f -> f.parameters().size() == 2).toList();
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected exactly"
                    + " one 2-arg boolean::and, found " + fns.size());
        }
        String v = p1.parameters().get(0);
        TypedSpec b1 = p1.body().get(0);
        TypedSpec b2 = p2.parameters().get(0).equals(v) ? p2.body().get(0)
                : Pipelines.rewriteRowReads(p2.body().get(0),
                        p2.parameters().get(0), Map.of(), Set.of(),
                        vv -> new TypedVariable(v, vv.info()));
        return new TypedLambda(p1.parameters(),
                List.of(new TypedNativeCall(fns.get(0), List.of(b1, b2),
                        b1.info())), p1.info());
    }


    private static boolean navBottomsAtVar(TypedSpec n) {
        return switch (n) {
            case TypedVariable ignored -> true;
            case TypedPropertyAccess pa ->
                    navBottomsAtVar(pa.source());
            case TypedMilestonedAccess ma ->
                    navBottomsAtVar(ma.source());
            case TypedFilter f -> navBottomsAtVar(f.source());
            case TypedNativeCall c
                    when c.args().size() == 1 && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName()) ->
                    navBottomsAtVar(c.args().get(0));
            default -> false;
        };
    }

    /** A synthetic head's underlying property name ({@code product#f0} /
     * {@code product#d1} → {@code product}); identity for ordinary heads.
     * DISPATCH-participating (Substitution's identity checks) — never
     * widen its vocabulary; message spelling belongs to
     * {@link #displayName}. */
    static String realHead(String head) {
        return JoinIdentity.of(head).prop();
    }

    /** MESSAGE-ONLY spelling of a head: strips the {@code #fN}/{@code #dN}
     * synthetics AND the subtype-dispatch {@code stc_..___} prefix (§2
     * hygiene — no internal identifier reaches a user-facing message).
     * Never consulted by dispatch. */
    static String displayName(String head) {
        if (com.legend.model.ClassMapping.isSubTypeColumn(head)) {
            return head.substring(head.indexOf("___") + 3);
        }
        return JoinIdentity.of(head).prop();
    }

    /** Apply the date splitter's verdicts in ONE identity-keyed pass
     * (rebuildChildren makes fresh nodes, so two sequential walks would
     * orphan the second map's identities): {@code strips} = CONTEXT-equal
     * dated accesses replaced by their PLAIN property equivalent (an
     * explicit date equal to the propagated context IS the propagation —
     * engine merge-by-identity — and must ride the ordinary propagation
     * channel, not the dated-fetch one); {@code renames} = foreign-dated
     * accesses renamed to date-fingerprinted synthetic heads. */
    TypedSpec replaceDatedNodes(TypedSpec n,
            IdentityHashMap<TypedSpec, String> renames,
            IdentityHashMap<TypedSpec, Boolean> strips) {
        if (strips.containsKey(n)) {
            var ma = (TypedMilestonedAccess) n;
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    replaceDatedNodes(ma.source(), renames, strips),
                    ma.property(), ma.info());
        }
        String newName = renames.get(n);
        if (newName != null) {
            var ma = (TypedMilestonedAccess) n;
            return new TypedMilestonedAccess(
                    replaceDatedNodes(ma.source(), renames, strips), newName,
                    ma.dates(), ma.sweep(), ma.info());
        }
        return rebuildChildren(n, c -> replaceDatedNodes(c, renames, strips));
    }

    /**
     * ONE-LEVEL generic rebuild: {@code f} applies to every child
     * expression (lambda bodies included; lambda/column structure is
     * preserved). Unknown node kinds pass through UNCHANGED — walkers
     * built on this are best-effort by design (an unvisited shape keeps
     * its loud downstream error, never silent SQL).
     */
    static TypedSpec rebuildChildren(TypedSpec n,
            UnaryOperator<TypedSpec> f) {
        return switch (n) {
            case TypedProject p ->
                    new TypedProject(
                            f.apply(p.source()),
                            p.columns().stream().map(c ->
                                    new TypedFuncCol(
                                            c.name(), (TypedLambda) f.apply(c.fn())))
                                    .toList(),
                            p.info());
            case TypedFilter fl -> new TypedFilter(f.apply(fl.source()),
                    (TypedLambda) f.apply(fl.predicate()), fl.info());
            case TypedGroupBy gb -> new TypedGroupBy(f.apply(gb.source()),
                    gb.keys().stream().map(k -> new TypedGroupBy.GroupKey(
                            k.column(), k.fn().map(fn -> (TypedLambda) f.apply(fn))))
                            .toList(),
                    gb.aggs().stream().map(a -> new TypedAggCol(a.name(),
                            (TypedLambda) f.apply(a.map()), a.reduce(),
                            a.order().stream().map(o -> new TypedAggCol.AggOrder(
                                    (TypedLambda) f.apply(o.key()), o.ascending(), o.nullOrder())).toList()))
                            .toList(),
                    gb.info());
            case TypedSortBy sb -> new TypedSortBy(f.apply(sb.source()),
                    (TypedLambda) f.apply(sb.key()), sb.ascending(),
                    sb.keyAlias(), sb.info());
            case com.legend.compiler.spec.typed.TypedSort so ->
                    new com.legend.compiler.spec.typed.TypedSort(
                            f.apply(so.source()), so.keys(),
                            so.pureNullOrder(), so.info());
            case TypedLimit l -> new TypedLimit(f.apply(l.source()),
                    l.count(), l.info());
            case TypedDrop d -> new TypedDrop(f.apply(d.source()),
                    d.count(), d.info());
            case TypedSlice sl -> new TypedSlice(f.apply(sl.source()),
                    sl.start(), sl.stop(), sl.info());
            case TypedFrom fr -> new TypedFrom(f.apply(fr.source()),
                    fr.context(), fr.executedExtent(), fr.info());
            case TypedLambda l -> new TypedLambda(l.parameters(),
                    l.body().stream().map(f).toList(), l.info());
            case TypedNativeCall c ->
                    c.withChildren(c.args().stream().map(f).toList());
            case TypedPropertyAccess pa ->
                    new TypedPropertyAccess(
                            f.apply(pa.source()), pa.property(), pa.info());
            case TypedMilestonedAccess ma ->
                    new TypedMilestonedAccess(
                            f.apply(ma.source()), ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
            case TypedMap m ->
                    new TypedMap(
                            f.apply(m.source()),
                            (TypedLambda) f.apply(m.mapper()), m.info());
            case TypedIf i ->
                    new TypedIf(
                            f.apply(i.condition()), f.apply(i.thenBranch()),
                            i.elseBranch().map(f), i.info());
            case TypedCollection c -> c.withChildren(c.elements().stream().map(f).toList());
            case TypedCast c ->
                    new TypedCast(
                            f.apply(c.source()), c.target(), c.info(),
                            c.wire());
            default -> n;
        };
    }

    /** Lifted filtered-navigation heads: synthetic name → the user
     * predicate parked on the head ({@link #liftFilteredHeads}).
     * Append-only across nested resolutions — names are counter-unique. */
    /** CORRELATED lifted predicates (read the OUTER lambda's row too):
     * applied at the association JOIN CONDITION, where both rows are in
     * scope — never at the target pipeline (audit 14 B-F1's correlation
     * pass, finally built). */
    private final Map<String, TypedLambda> corrPreds =
            new java.util.LinkedHashMap<>();

    /** POSITIONAL heads ({@code #pN}) → the picked index k. */
    private final Map<String, Integer> positional = new LinkedHashMap<>();
    private final Map<String, TypedLambda> preds =
            new LinkedHashMap<>();

    /** {@code #cN} heads: synthetic name → the ORDERED branch predicates
     * (null members = unfiltered branches). */
    private final Map<String, List<TypedLambda>> branchPreds =
            new LinkedHashMap<>();

    /** (prop, branch predicates) → minted {@code #cN} name: the same
     * stream expression appearing twice shares ONE join identity. */
    private final Map<List<Object>, String> concatMemo =
            new LinkedHashMap<>();

    /** {@code #uN} heads: synthetic name → the parked branch chains. */
    private final Map<String, UnionSpec> unionSpecs = new LinkedHashMap<>();

    /** (variable, leaf class, branch paths) → minted {@code #uN} name. */
    private final Map<List<Object>, String> unionMemo = new LinkedHashMap<>();

    /** Alpha-normalized predicate for identity comparison: separate
     * β-inlines of the same derived property differ only in the fresh
     * parameter name — rename to a fixed one so record equality sees
     * through it. */
    private static TypedLambda canonicalPred(TypedLambda pred) {
        // FULL alpha-normalization (audit 16): the top-level rename alone
        // let nested-lambda fresh names (_iN from separate β-inlines of one
        // derived property) defeat the memo — two identities, two joins,
        // row multiplication. Canonical names contain '#', unspellable as
        // pure variables, so user code can never capture them.
        return (TypedLambda) alphaNormalize(pred,
                new LinkedHashMap<>(), new int[]{0});
    }

    private static TypedSpec alphaNormalize(@com.legend.Nullable TypedSpec n,
            Map<String, String> env, int[] counter) {
        if (n instanceof TypedVariable v) {
            String canonical = env.get(v.name());
            return canonical == null ? v
                    : new TypedVariable(canonical, v.info());
        }
        if (n instanceof TypedLambda l) {
            Map<String, String> inner = new LinkedHashMap<>(env);
            List<String> ps = new java.util.ArrayList<>(l.parameters().size());
            for (String p : l.parameters()) {
                String c = "#a" + counter[0]++;
                inner.put(p, c);
                ps.add(c);
            }
            return new TypedLambda(ps,
                    l.body().stream()
                            .map(b -> alphaNormalize(b, inner, counter))
                            .toList(),
                    l.info());
        }
        return rebuildChildren(java.util.Objects.requireNonNull(n, "n"),
                c -> alphaNormalize(c, env, counter));
    }

    /** The variable a liftable navigation chain bottoms at. */
    private static String bottomVarOf(@com.legend.Nullable TypedSpec n) {
        return switch (n) {
            case TypedVariable v -> v.name();
            case TypedPropertyAccess pa -> bottomVarOf(pa.source());
            case TypedMilestonedAccess ma -> bottomVarOf(ma.source());
            case TypedFilter f -> bottomVarOf(f.source());
            case TypedNativeCall c when c.args().size() == 1 ->
                    bottomVarOf(c.args().get(0));
            case null, default -> throw new IllegalStateException(
                    "resolver bug: liftable nav does not bottom at a variable");
        };
    }

    private int count = 0;

    /** Column lambdas born from VALUES-position map terminals: pure
     * flattening drops empties there, so the TDS lift (whose LEFT-join
     * NULL row is the point) must NOT fire inside them.
     * IDENTITY-keyed (audit 23 residual, documented): the gate holds only
     * while no pass REBUILDS the column lambda between registration and
     * the lift — a rebuilt (structurally-equal, identity-different)
     * lambda would silently take the TDS lift and emit a NULL row where
     * pure flattening drops it. Registration and consumption sit in THIS
     * class within one liftFilteredHeads walk; keep it that way. */
    private final Set<TypedLambda> valuesLambdas =
            Collections.newSetFromMap(new IdentityHashMap<>());
    /** A lifted head's (and a drilled synthetic MID component's) predicate
     * reads are TAILS too: they pull the target's own slots exactly like
     * demanded leaves. */
    List<List<String>> predTailsFor(List<String> path, int mid) {
        List<List<String>> predTails = new java.util.ArrayList<>();
        Set<String> predComponents = new java.util.LinkedHashSet<>();
        predComponents.add(path.get(0));
        if (mid > 1) {
            predComponents.add(path.get(mid - 1));
        }
        for (String pcpt : predComponents) {
            for (TypedLambda liftedPred : allPreds(pcpt)) {
                Set<List<String>> predPaths = new java.util.LinkedHashSet<>();
                for (TypedSpec b : liftedPred.body()) {
                    FlattenOps.consumedPaths(b, liftedPred.parameters().get(0),
                            predPaths);
                }
                predTails.addAll(predPaths);
            }
        }
        return predTails;
    }


    /** #69 (audit-22 follow-on): a CORRELATED pred's OUTER-variable
     * reads are PARENT demand — the lift moved the only occurrence of
     * {@code $f.<head>...} out of the projection column, so the ordinary
     * scans no longer see it and the head's navigate material never
     * registered (the Substitution 'class-typed slot' wall family). */
    void corrPredOuterDemand(TypedLambda fn, Set<List<String>> out) {
        if (fn.parameters().isEmpty()) {
            return;
        }
        String uv = fn.parameters().get(0);
        for (TypedLambda corr : allCorrelatedPreds()) {
            for (TypedSpec b : corr.body()) {
                FlattenOps.consumedPaths(b, uv, out);
            }
        }
    }


}
